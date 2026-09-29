"""Behaviour of the write path, recovery, and query semantics."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from poorgraph.demo import CDC_IDENTIFIER_QUARANTINED, SAMPLE_BULK
from poorgraph.storage import GraphStore, _CrashBetweenPhases


def node_event(item, version=1, prefix="n"):
    return {
        "event_id": f"{prefix}:{item['type']}:{item['id']}:v{version}",
        "op": "upsert",
        "entity_kind": "node",
        "entity_type": item["type"],
        "entity_id": item["id"],
        "version": version,
        "properties": item["properties"],
    }


def edge_event(item, version=1, prefix="e"):
    return {
        "event_id": f"{prefix}:{item['type']}:{item['id']}:v{version}",
        "op": "upsert",
        "entity_kind": "edge",
        "entity_type": item["type"],
        "entity_id": item["id"],
        "version": version,
        "source": item["source"],
        "target": item["target"],
        "properties": item["properties"],
    }


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test.db"
        self.store = GraphStore(self.db_path)

    def tearDown(self):
        self.store.close()
        self.tmpdir.cleanup()

    def fresh(self, name="other.db"):
        store = GraphStore(Path(self.tmpdir.name) / name)
        self.addCleanup(store.close)
        return store


class QueryTests(StoreTestCase):
    def setUp(self):
        super().setUp()
        self.store.bulk_load(SAMPLE_BULK)

    def test_shortest_path(self):
        result = self.store.shortest_path(
            {
                "source": "Profile:p_alice",
                "target": "Campaign:camp_spring_sale",
                "direction": "both",
                "max_depth": 4,
            }
        )
        self.assertTrue(result["found"])
        self.assertEqual(
            result["node_ids"],
            ["Profile:p_alice", "Segment:seg_high_intent", "Campaign:camp_spring_sale"],
        )
        self.assertEqual(
            result["edge_ids"],
            ["PROFILE_IN_SEGMENT:p_alice-high-intent", "CAMPAIGN_TARGETS_SEGMENT:spring-high-intent"],
        )

    def test_neighborhood_returns_an_induced_subgraph(self):
        result = self.store.neighborhood({"start": "Profile:p_alice", "depth": 1, "direction": "out"})
        node_ids = {n["id"] for n in result["nodes"]}
        edge_ids = {e["id"] for e in result["edges"]}
        self.assertEqual(
            node_ids,
            {
                "Profile:p_alice",
                "Identifier:email_alice_hash",
                "Identifier:cookie_alice_web",
                "Identifier:maid_alice_phone",
                "Identifier:cookie_shared_device",
                "Segment:seg_high_intent",
            },
        )
        # BFS did not traverse this at depth 1, but both endpoints are in the
        # result. Returning the induced edge preserves the real topology.
        self.assertIn("IDENTIFIER_OBSERVED_WITH:cookie-email-alice", edge_ids)
        self.assertTrue(result["induced"])

    def test_neighborhood_can_return_the_bfs_tree_instead(self):
        result = self.store.neighborhood(
            {"start": "Profile:p_alice", "depth": 1, "direction": "out", "induced": False}
        )
        self.assertNotIn(
            "IDENTIFIER_OBSERVED_WITH:cookie-email-alice", {e["id"] for e in result["edges"]}
        )

    def test_registry_exposes_algorithms_without_core_changes(self):
        names = {a["name"] for a in self.store.algorithms()}
        self.assertEqual(
            names, {"neighborhood", "shortest_path", "connected_component", "degree_ranking"}
        )

    def test_algorithms_reachable_by_name(self):
        component = self.store.query("connected_component", {"start": "Segment:seg_high_intent"})
        self.assertEqual(component["size"], 12)
        self.assertEqual(component["node_types"]["Profile"], 2)
        ranking = self.store.query("degree_ranking", {"k": 1, "node_type": "Identifier"})
        self.assertEqual(ranking["top"][0]["id"], "Identifier:cookie_shared_device")

    def test_hyphenated_algorithm_name_is_accepted(self):
        self.assertTrue(
            self.store.query(
                "shortest-path",
                {
                    "source": "Identifier:cookie_alice_web",
                    "target": "Campaign:camp_spring_sale",
                    "direction": "both",
                    "max_depth": 5,
                },
            )["found"]
        )

    def test_unknown_algorithm_raises_keyerror(self):
        with self.assertRaises(KeyError):
            self.store.query("pagerank", {})


class ConvergenceTests(StoreTestCase):
    """The brief's hard requirement: both ingestion paths reach the same graph."""

    def _snapshot(self, store):
        nodes = {n: (r.node_type, r.properties) for n, r in store.graph.nodes.items()}
        edges = {
            e: (r.edge_type, r.source_node_id, r.target_node_id) for e, r in store.graph.edges.items()
        }
        return nodes, edges

    def test_bulk_and_in_order_cdc_converge(self):
        self.store.bulk_load(SAMPLE_BULK)
        cdc = self.fresh("cdc.db")
        for item in SAMPLE_BULK["nodes"]:
            cdc.apply_cdc(node_event(item))
        for item in SAMPLE_BULK["edges"]:
            cdc.apply_cdc(edge_event(item))
        self.assertEqual(self._snapshot(self.store), self._snapshot(cdc))

    def test_bulk_and_reversed_out_of_order_cdc_converge(self):
        """Edges before nodes -- the normal case for independent CDC streams."""
        self.store.bulk_load(SAMPLE_BULK)
        cdc = self.fresh("ooo.db")
        for item in SAMPLE_BULK["edges"]:
            cdc.apply_cdc(edge_event(item))
        self.assertEqual(cdc.graph.stats()["edges"], 0)
        self.assertEqual(cdc.graph.stats()["deferred_edges"], len(SAMPLE_BULK["edges"]))
        for item in SAMPLE_BULK["nodes"]:
            cdc.apply_cdc(node_event(item))
        self.assertEqual(cdc.graph.stats()["deferred_edges"], 0)
        self.assertEqual(self._snapshot(self.store), self._snapshot(cdc))


class WriteGuaranteeTests(StoreTestCase):
    def test_idempotent_by_event_id(self):
        self.store.bulk_load(SAMPLE_BULK)
        first = self.store.apply_cdc(CDC_IDENTIFIER_QUARANTINED)
        second = self.store.apply_cdc(CDC_IDENTIFIER_QUARANTINED)
        self.assertTrue(first["state_changed"])
        self.assertFalse(second["state_changed"])
        self.assertEqual(second["reason"], "duplicate_event")

    def test_stale_event_is_retained_in_the_wal_and_skipped(self):
        self.store.bulk_load(SAMPLE_BULK)
        self.store.apply_cdc(CDC_IDENTIFIER_QUARANTINED)
        stale = {
            **CDC_IDENTIFIER_QUARANTINED,
            "event_id": "identifiers:cookie_shared_device:v1-late",
            "version": 1,
            "properties": {
                "kind": "cookie",
                "value_hash": "hash_cookie_shared_device",
                "status": "active",
            },
        }
        result = self.store.apply_cdc(stale)
        self.assertEqual(result["reason"], "stale_version")
        self.assertEqual(
            self.store.graph.nodes["Identifier:cookie_shared_device"].properties["status"],
            "quarantined",
        )
        row = self.store.conn.execute(
            "SELECT status, skipped_reason FROM graph_wal WHERE event_id = ?",
            ("identifiers:cookie_shared_device:v1-late",),
        ).fetchone()
        self.assertEqual((row["status"], row["skipped_reason"]), (2, "stale_version"))

    def test_one_bad_event_does_not_discard_its_valid_siblings(self):
        batch = {
            "batch_id": "mixed",
            "nodes": [
                {"type": "Profile", "id": "good1", "properties": {"status": "active", "created_at": "2026-09-01"}},
                {"type": "Profile", "id": "good2", "properties": {"status": "active", "created_at": "2026-09-01"}},
                {"type": "Profile", "id": "bad", "properties": {"created_at": "2026-09-01"}},
                {"type": "Profile", "id": "good3", "properties": {"status": "active", "created_at": "2026-09-01"}},
            ],
        }
        report = self.store.bulk_load(batch)
        self.assertEqual(report["applied"], 3)
        self.assertEqual(self.store.graph.stats()["nodes"], 3)
        row = self.store.conn.execute(
            "SELECT status, skipped_reason FROM graph_wal WHERE entity_external_id = 'bad'"
        ).fetchone()
        self.assertEqual(row["status"], 2)
        self.assertIn("status", row["skipped_reason"])

    def test_malformed_envelope_is_reported_not_raised(self):
        result = self.store.apply_cdc({"entity_kind": "node", "type": "Profile", "id": "x"})
        self.assertEqual(result["reason"], "rejected_envelope")
        self.assertIn("event_id", result["detail"])

    def test_node_delete_then_recreate_restores_its_edges(self):
        self.store.bulk_load(SAMPLE_BULK)
        before = self.store.graph.stats()["edges"]
        self.store.apply_cdc(
            {
                "event_id": "del-p-alice",
                "op": "delete",
                "entity_kind": "node",
                "entity_type": "Profile",
                "entity_id": "p_alice",
                "version": 2,
            }
        )
        self.assertLess(self.store.graph.stats()["edges"], before)
        self.store.apply_cdc(
            {
                "event_id": "recreate-p-alice",
                "op": "upsert",
                "entity_kind": "node",
                "entity_type": "Profile",
                "entity_id": "p_alice",
                "version": 3,
                "properties": {
                    "status": "active",
                    "created_at": "2026-09-01",
                    "home_market": "US",
                    "confidence": 0.96,
                },
            }
        )
        self.assertEqual(self.store.graph.stats()["edges"], before)
        self.assertTrue(
            self.store.shortest_path(
                {
                    "source": "Profile:p_alice",
                    "target": "Campaign:camp_spring_sale",
                    "direction": "both",
                }
            )["found"]
        )

    def test_explicit_edge_delete_removes_it(self):
        self.store.bulk_load(SAMPLE_BULK)
        result = self.store.apply_cdc(
            {
                "event_id": "edges:p-alice-segment:v2-delete",
                "op": "delete",
                "entity_kind": "edge",
                "entity_type": "PROFILE_IN_SEGMENT",
                "entity_id": "p_alice-high-intent",
                "version": 2,
            }
        )
        self.assertTrue(result["state_changed"])
        self.assertFalse(
            self.store.shortest_path(
                {
                    "source": "Profile:p_alice",
                    "target": "Campaign:camp_spring_sale",
                    "direction": "both",
                }
            )["found"]
        )

    def test_durable_tables_and_index_agree(self):
        self.store.bulk_load(SAMPLE_BULK)
        self.assertTrue(self.store.stats()["consistent"])


class RecoveryTests(StoreTestCase):
    def test_rehydrate_after_clean_restart(self):
        self.store.bulk_load(SAMPLE_BULK)
        self.store.apply_cdc(CDC_IDENTIFIER_QUARANTINED)
        self.store.close()
        restarted = GraphStore(self.db_path)
        self.addCleanup(restarted.close)
        self.assertTrue(
            restarted.shortest_path(
                {
                    "source": "Profile:p_alice",
                    "target": "Campaign:camp_spring_sale",
                    "direction": "both",
                }
            )["found"]
        )
        self.assertEqual(
            restarted.graph.nodes["Identifier:cookie_shared_device"].properties["status"],
            "quarantined",
        )
        self.assertEqual(restarted.recovery["wal_events_replayed"], 0)

    def test_crash_between_wal_commit_and_materialization_is_replayed(self):
        self.store.bulk_load(SAMPLE_BULK)
        self.store.crash_after_wal_append = True
        with self.assertRaises(_CrashBetweenPhases):
            self.store.apply_cdc(CDC_IDENTIFIER_QUARANTINED)

        self.assertEqual(
            self.store.graph.nodes["Identifier:cookie_shared_device"].properties["status"], "review"
        )
        pending = self.store.conn.execute(
            "SELECT COUNT(*) FROM graph_wal WHERE status = 0"
        ).fetchone()[0]
        self.assertEqual(pending, 1)
        self.store.close()

        recovered = GraphStore(self.db_path)
        self.addCleanup(recovered.close)
        self.assertEqual(recovered.recovery["wal_events_replayed"], 1)
        self.assertEqual(recovered.recovery["wal_events_still_pending"], 0)
        self.assertEqual(
            recovered.graph.nodes["Identifier:cookie_shared_device"].properties["status"],
            "quarantined",
        )

    def test_replay_is_idempotent_across_repeated_restarts(self):
        self.store.bulk_load(SAMPLE_BULK)
        self.store.close()
        for _ in range(3):
            store = GraphStore(self.db_path)
            stats = store.stats()
            store.close()
            self.assertEqual(stats["in_memory"]["nodes"], len(SAMPLE_BULK["nodes"]))
            self.assertEqual(stats["in_memory"]["edges"], len(SAMPLE_BULK["edges"]))
            self.assertTrue(stats["consistent"])

    def test_storage_format_mismatch_fails_loudly(self):
        from poorgraph.storage import StorageFormatError

        self.store.bulk_load(SAMPLE_BULK)
        self.store.conn.execute("PRAGMA user_version = 99")
        self.store.conn.commit()
        self.store.close()
        with self.assertRaises(StorageFormatError):
            GraphStore(self.db_path)


class SchemaTests(StoreTestCase):
    def test_edge_endpoint_types_are_enforced(self):
        self.store.bulk_load(SAMPLE_BULK)
        result = self.store.apply_cdc(
            {
                "event_id": "bad-endpoints",
                "op": "upsert",
                "entity_kind": "edge",
                "entity_type": "PROFILE_HAS_IDENTIFIER",
                "entity_id": "wrong",
                "version": 1,
                "source": {"type": "Identifier", "id": "cookie_alice_web"},
                "target": {"type": "Profile", "id": "p_alice"},
                "properties": {},
            }
        )
        self.assertFalse(result["state_changed"])
        self.assertEqual(result["reason"], "schemaerror")

    def test_unknown_property_is_forward_compatible(self):
        result = self.store.apply_cdc(
            {
                "event_id": "future-field",
                "op": "upsert",
                "entity_kind": "node",
                "entity_type": "Profile",
                "entity_id": "p_future",
                "version": 1,
                "properties": {
                    "status": "active",
                    "created_at": "2026-09-01",
                    "new_score_from_source": 12,
                },
            }
        )
        self.assertTrue(result["state_changed"])

    def test_schema_drift_is_visible(self):
        self.assertFalse(self.store.recovery["schema_drift"]["drift_detected"])
        self.assertIn("fingerprint", self.store.schema())


if __name__ == "__main__":
    unittest.main()
