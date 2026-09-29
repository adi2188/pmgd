"""Durable graph storage and write path."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .algorithms import catalog as algorithm_catalog
from .algorithms import run as run_algorithm
from .graph import EdgeRecord, InMemoryGraph, NodeRecord
from .schema import (
    SCHEMA_VERSION,
    SchemaError,
    edge_key,
    node_key,
    schema_as_dict,
    schema_fingerprint,
    validate_edge,
    validate_node,
)


STORAGE_FORMAT_VERSION = 1
STATUS_PENDING = 0
STATUS_APPLIED = 1
STATUS_SKIPPED = 2
PENDING_REASON_PREFIX = "pending_missing_endpoint"


class GraphConflict(ValueError):
    """Raised when a valid event cannot be applied to the current graph."""


class StorageFormatError(RuntimeError):
    """Raised when the database format is not one this server can read."""


class _CrashBetweenPhases(RuntimeError):
    """Test hook: the event log is durable but materialization did not happen."""


@dataclass(frozen=True)
class MaterializeResult:
    changed: bool
    status: int
    reason: str | None = None
    detail: str | None = None


class GraphStore:
    """Owns SQLite persistence and the rebuildable in-memory graph index."""

    def __init__(self, db_path: str | Path = "poorgraph.db", durability: str = "strict") -> None:
        if durability not in {"strict", "relaxed"}:
            raise ValueError("durability must be one of: strict, relaxed")
        self.db_path = Path(db_path)
        self.durability = durability
        self._lock = threading.RLock()
        self.crash_after_wal_append = False
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute(f"PRAGMA synchronous = {'FULL' if durability == 'strict' else 'NORMAL'}")
        self._check_storage_format()
        self._init_db()
        schema_drift = self._record_schema()
        replay = self._replay_pending_wal(apply_to_index=False)
        self.recovery = {
            "schema_drift": schema_drift,
            "wal_events_replayed": replay["replayed"],
            "wal_events_still_pending": replay["still_pending"],
        }
        self.graph = self.rehydrate()

    def close(self) -> None:
        self.conn.close()

    def _check_storage_format(self) -> None:
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, STORAGE_FORMAT_VERSION):
            raise StorageFormatError(
                f"database format {version} is not supported by format {STORAGE_FORMAT_VERSION}"
            )
        if version == 0:
            self.conn.execute(f"PRAGMA user_version = {STORAGE_FORMAT_VERSION}")
            self.conn.commit()

    def _init_db(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_versions (
                version INTEGER NOT NULL,
                fingerprint TEXT NOT NULL,
                applied_at INTEGER NOT NULL,
                schema_json TEXT NOT NULL,
                PRIMARY KEY (version, fingerprint)
            );

            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                node_type TEXT NOT NULL,
                external_id TEXT NOT NULL,
                properties_json TEXT NOT NULL,
                entity_version INTEGER NOT NULL,
                deleted_at_version INTEGER
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_type_external
            ON nodes(node_type, external_id);

            CREATE TABLE IF NOT EXISTS edges (
                edge_id TEXT PRIMARY KEY,
                edge_type TEXT NOT NULL,
                external_id TEXT NOT NULL,
                source_node_id TEXT NOT NULL,
                target_node_id TEXT NOT NULL,
                properties_json TEXT NOT NULL,
                entity_version INTEGER NOT NULL,
                deleted_at_version INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_node_id);
            CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_node_id);
            CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(edge_type);

            CREATE TABLE IF NOT EXISTS graph_wal (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                received_at INTEGER NOT NULL,
                op TEXT NOT NULL,
                entity_kind TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_external_id TEXT NOT NULL,
                entity_version INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                status INTEGER NOT NULL DEFAULT 0,
                skipped_reason TEXT
            );
            """
        )
        self.conn.commit()

    def _record_schema(self) -> dict[str, Any]:
        schema = schema_as_dict()
        fingerprint = schema_fingerprint()
        self.conn.execute(
            """
            INSERT OR IGNORE INTO schema_versions(version, fingerprint, applied_at, schema_json)
            VALUES (?, ?, ?, ?)
            """,
            (SCHEMA_VERSION, fingerprint, int(time.time()), json.dumps(schema, sort_keys=True)),
        )
        self.conn.commit()
        rows = self.conn.execute(
            "SELECT fingerprint FROM schema_versions WHERE version = ? ORDER BY fingerprint",
            (SCHEMA_VERSION,),
        ).fetchall()
        fingerprints = [row["fingerprint"] for row in rows]
        drifted = any(item != fingerprint for item in fingerprints)
        return {
            "drift_detected": drifted,
            "current_version": SCHEMA_VERSION,
            "fingerprint": fingerprint,
            "recorded_fingerprints": fingerprints,
        }

    def rehydrate(self) -> InMemoryGraph:
        with self._lock:
            node_rows = self.conn.execute(
                """
                SELECT node_id, node_type, external_id, properties_json, entity_version
                FROM nodes
                WHERE deleted_at_version IS NULL
                ORDER BY node_id
                """
            ).fetchall()
            nodes = [
                NodeRecord(
                    node_id=row["node_id"],
                    node_type=row["node_type"],
                    external_id=row["external_id"],
                    properties=json.loads(row["properties_json"]),
                    version=row["entity_version"],
                )
                for row in node_rows
            ]
            edge_rows = self.conn.execute(
                """
                SELECT edge_id, edge_type, source_node_id, target_node_id, properties_json, entity_version
                FROM edges
                WHERE deleted_at_version IS NULL
                ORDER BY edge_id
                """
            ).fetchall()
            edges = [
                EdgeRecord(
                    edge_id=row["edge_id"],
                    edge_type=row["edge_type"],
                    source_node_id=row["source_node_id"],
                    target_node_id=row["target_node_id"],
                    properties=json.loads(row["properties_json"]),
                    version=row["entity_version"],
                )
                for row in edge_rows
            ]
        graph = InMemoryGraph()
        graph.bulk_install(nodes, edges)
        return graph

    def schema(self) -> dict[str, Any]:
        return schema_as_dict()

    def algorithms(self) -> list[dict[str, Any]]:
        return algorithm_catalog()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            durable = {
                "nodes_total": self.conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0],
                "nodes_live": self.conn.execute(
                    "SELECT COUNT(*) FROM nodes WHERE deleted_at_version IS NULL"
                ).fetchone()[0],
                "edges_total": self.conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0],
                "edges_explicitly_live": self.conn.execute(
                    "SELECT COUNT(*) FROM edges WHERE deleted_at_version IS NULL"
                ).fetchone()[0],
                "edges_query_live": self._query_live_edge_count(),
                "wal_events": self.conn.execute("SELECT COUNT(*) FROM graph_wal").fetchone()[0],
                "wal_pending_events": self.conn.execute(
                    "SELECT COUNT(*) FROM graph_wal WHERE status = ?", (STATUS_PENDING,)
                ).fetchone()[0],
                "wal_skipped_events": self.conn.execute(
                    "SELECT COUNT(*) FROM graph_wal WHERE status = ?", (STATUS_SKIPPED,)
                ).fetchone()[0],
            }
        return {
            "durable": durable,
            "in_memory": self.graph.stats(),
            "consistent": self._is_consistent(durable),
        }

    def _query_live_edge_count(self) -> int:
        return self.conn.execute(
            """
            SELECT COUNT(*)
            FROM edges e
            JOIN nodes s ON s.node_id = e.source_node_id AND s.deleted_at_version IS NULL
            JOIN nodes t ON t.node_id = e.target_node_id AND t.deleted_at_version IS NULL
            WHERE e.deleted_at_version IS NULL
            """
        ).fetchone()[0]

    def _is_consistent(self, durable: dict[str, int] | None = None) -> bool:
        if durable is None:
            durable = {
                "nodes_live": self.conn.execute(
                    "SELECT COUNT(*) FROM nodes WHERE deleted_at_version IS NULL"
                ).fetchone()[0],
                "edges_explicitly_live": self.conn.execute(
                    "SELECT COUNT(*) FROM edges WHERE deleted_at_version IS NULL"
                ).fetchone()[0],
                "edges_query_live": self._query_live_edge_count(),
            }
        in_memory = self.graph.stats()
        expected_deferred = durable["edges_explicitly_live"] - durable["edges_query_live"]
        return (
            in_memory["nodes"] == durable["nodes_live"]
            and in_memory["edges"] == durable["edges_query_live"]
            and in_memory["deferred_edges"] == expected_deferred
        )

    def bulk_load(self, batch: dict[str, Any]) -> dict[str, Any]:
        batch_id = str(batch.get("batch_id") or f"bulk-{int(time.time())}")
        version = int(batch.get("version", 1))
        events: list[dict[str, Any]] = []

        for item in batch.get("nodes", []):
            entity_type = item["type"]
            external_id = item["id"]
            item_version = int(item.get("version", version))
            events.append(
                {
                    "event_id": item.get(
                        "event_id",
                        f"{batch_id}:node:{entity_type}:{external_id}:v{item_version}",
                    ),
                    "op": item.get("op", "upsert"),
                    "entity_kind": "node",
                    "entity_type": entity_type,
                    "entity_id": external_id,
                    "version": item_version,
                    "properties": item.get("properties", {}),
                }
            )

        for item in batch.get("edges", []):
            entity_type = item["type"]
            external_id = item["id"]
            item_version = int(item.get("version", version))
            events.append(
                {
                    "event_id": item.get(
                        "event_id",
                        f"{batch_id}:edge:{entity_type}:{external_id}:v{item_version}",
                    ),
                    "op": item.get("op", "upsert"),
                    "entity_kind": "edge",
                    "entity_type": entity_type,
                    "entity_id": external_id,
                    "version": item_version,
                    "source": item["source"],
                    "target": item["target"],
                    "properties": item.get("properties", {}),
                }
            )

        results = self.apply_events(events)
        return {
            "batch_id": batch_id,
            "events": len(events),
            "applied": sum(1 for result in results if result["state_changed"]),
            "results": results,
        }

    def apply_cdc(self, event: dict[str, Any]) -> dict[str, Any]:
        return self.apply_events([event])[0]

    def apply_events(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        with self._lock:
            pending_count = self._pending_count()
            for raw_event in events:
                result = self._append_and_try_materialize(raw_event)
                replayed_count = 0
                if result.get("status") == STATUS_PENDING:
                    pending_count += 1
                elif result.get("state_changed") and pending_count > 0:
                    replayed = self._replay_pending_wal(apply_to_index=True)
                    replayed_count = replayed["replayed"]
                    pending_count = replayed["still_pending"]
                result["replayed_pending"] = replayed_count
                result["still_pending"] = pending_count
                results.append(result)
        return results

    def query(self, name: str, query: dict[str, Any]) -> dict[str, Any]:
        return run_algorithm(self.graph, name, query)

    def neighborhood(self, query: dict[str, Any]) -> dict[str, Any]:
        return self.query("neighborhood", query)

    def shortest_path(self, query: dict[str, Any]) -> dict[str, Any]:
        return self.query("shortest_path", query)

    def _append_and_try_materialize(self, raw_event: dict[str, Any]) -> dict[str, Any]:
        try:
            event = self._normalize_event(raw_event)
        except Exception as exc:
            return {
                "event_id": str(raw_event.get("event_id", "")) if isinstance(raw_event, dict) else "",
                "wal_sequence": None,
                "state_changed": False,
                "reason": "rejected_envelope",
                "detail": str(exc),
            }

        append_result = self._append_event(event)
        if append_result["reason"] == "duplicate_event":
            return append_result
        if self.crash_after_wal_append:
            self.crash_after_wal_append = False
            raise _CrashBetweenPhases(
                f"crashed after WAL append for {event['event_id']} before materialization"
            )

        result = self._try_materialize_sequence(
            append_result["wal_sequence"], event, apply_to_index=True, allow_equal_version=False
        )
        result["event_id"] = event["event_id"]
        result["wal_sequence"] = append_result["wal_sequence"]
        return result

    def _normalize_event(self, raw_event: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(raw_event, dict):
            raise ValueError("event must be a JSON object")
        entity_kind = raw_event.get("entity_kind", raw_event.get("entity"))
        entity_id = raw_event.get("entity_id", raw_event.get("id"))
        entity_type = raw_event.get("entity_type", raw_event.get("type"))
        version = int(raw_event.get("version", raw_event.get("entity_version", 1)))
        op = raw_event.get("op", "upsert")

        if op not in {"upsert", "delete"}:
            raise ValueError("op must be one of: upsert, delete")
        if entity_kind not in {"node", "edge"}:
            raise ValueError("entity_kind must be one of: node, edge")
        if not raw_event.get("event_id"):
            raise ValueError("event_id is required for idempotency")
        if not entity_type:
            raise ValueError("entity_type/type is required")
        if not entity_id:
            raise ValueError("entity_id/id is required")
        if version < 1:
            raise ValueError("version must be >= 1")

        event = {
            "event_id": str(raw_event["event_id"]),
            "op": op,
            "entity_kind": entity_kind,
            "entity_type": str(entity_type),
            "entity_id": str(entity_id),
            "version": version,
            "properties": raw_event.get("properties", {}),
            "raw": raw_event,
        }

        if entity_kind == "edge" and op == "upsert":
            if "source" not in raw_event or "target" not in raw_event:
                raise ValueError("edge upserts require source and target")
            event["source"] = self._normalize_endpoint(raw_event["source"])
            event["target"] = self._normalize_endpoint(raw_event["target"])
        return event

    @staticmethod
    def _normalize_endpoint(endpoint: dict[str, Any]) -> dict[str, str]:
        endpoint_type = endpoint.get("type")
        endpoint_id = endpoint.get("id")
        if not endpoint_type or not endpoint_id:
            raise ValueError("edge endpoint requires type and id")
        return {"type": str(endpoint_type), "id": str(endpoint_id)}

    def _append_event(self, event: dict[str, Any]) -> dict[str, Any]:
        payload_json = json.dumps(event["raw"], sort_keys=True)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO graph_wal(
                    event_id,
                    received_at,
                    op,
                    entity_kind,
                    entity_type,
                    entity_external_id,
                    entity_version,
                    payload_json,
                    status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event["event_id"],
                    int(time.time()),
                    event["op"],
                    event["entity_kind"],
                    event["entity_type"],
                    event["entity_id"],
                    event["version"],
                    payload_json,
                    STATUS_PENDING,
                ),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        existing = self.conn.execute(
            "SELECT sequence FROM graph_wal WHERE event_id = ?",
            (event["event_id"],),
        ).fetchone()
        if cursor.rowcount == 0:
            return {
                "event_id": event["event_id"],
                "wal_sequence": existing["sequence"],
                "state_changed": False,
                "reason": "duplicate_event",
            }
        return {
            "event_id": event["event_id"],
            "wal_sequence": existing["sequence"],
            "state_changed": False,
            "reason": "logged",
        }

    def _try_materialize_sequence(
        self,
        sequence: int,
        event: dict[str, Any],
        apply_to_index: bool,
        allow_equal_version: bool,
    ) -> dict[str, Any]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            materialized = self._materialize(event, allow_equal_version=allow_equal_version)
            self.conn.execute(
                """
                UPDATE graph_wal
                SET status = ?, skipped_reason = ?
                WHERE sequence = ?
                """,
                (materialized.status, materialized.reason, sequence),
            )
            self.conn.commit()
        except (GraphConflict, SchemaError, ValueError) as exc:
            self.conn.rollback()
            reason = type(exc).__name__.lower()
            detail = str(exc)
            self._mark_wal(sequence, STATUS_SKIPPED, f"{reason}: {detail}")
            return {"state_changed": False, "reason": reason, "detail": detail}
        except Exception:
            self.conn.rollback()
            raise

        if materialized.changed and apply_to_index:
            self._apply_to_index(event)
        return {
            "state_changed": materialized.changed,
            "reason": materialized.reason,
            "detail": materialized.detail,
            "status": materialized.status,
        }

    def _mark_wal(self, sequence: int, status: int, skipped_reason: str | None) -> None:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                """
                UPDATE graph_wal
                SET status = ?, skipped_reason = ?
                WHERE sequence = ?
                """,
                (status, skipped_reason, sequence),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _replay_pending_wal(self, apply_to_index: bool) -> dict[str, int]:
        rows = self.conn.execute(
            """
            SELECT sequence, payload_json
            FROM graph_wal
            WHERE status = ?
            ORDER BY sequence
            """,
            (STATUS_PENDING,),
        ).fetchall()
        replayed = 0
        for row in rows:
            event = self._normalize_event(json.loads(row["payload_json"]))
            result = self._try_materialize_sequence(
                row["sequence"], event, apply_to_index=apply_to_index, allow_equal_version=True
            )
            if result["state_changed"] and result["reason"] != PENDING_REASON_PREFIX:
                replayed += 1
        return {"replayed": replayed, "still_pending": self._pending_count()}

    def _pending_count(self) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM graph_wal WHERE status = ?", (STATUS_PENDING,)
        ).fetchone()[0]

    def _materialize(self, event: dict[str, Any], allow_equal_version: bool) -> MaterializeResult:
        if event["entity_kind"] == "node":
            return self._materialize_node(event, allow_equal_version=allow_equal_version)
        return self._materialize_edge(event, allow_equal_version=allow_equal_version)

    def _materialize_node(self, event: dict[str, Any], allow_equal_version: bool) -> MaterializeResult:
        record_id = node_key(event["entity_type"], event["entity_id"])
        current = self.conn.execute(
            "SELECT entity_version FROM nodes WHERE node_id = ?",
            (record_id,),
        ).fetchone()
        if current:
            if current["entity_version"] > event["version"]:
                return MaterializeResult(False, STATUS_SKIPPED, "stale_version")
            if current["entity_version"] == event["version"] and not allow_equal_version:
                return MaterializeResult(False, STATUS_SKIPPED, "stale_version")

        if event["op"] == "delete":
            self.conn.execute(
                """
                INSERT INTO nodes(node_id, node_type, external_id, properties_json, entity_version, deleted_at_version)
                VALUES (?, ?, ?, '{}', ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    entity_version = excluded.entity_version,
                    deleted_at_version = excluded.deleted_at_version
                """,
                (record_id, event["entity_type"], event["entity_id"], event["version"], event["version"]),
            )
            return MaterializeResult(True, STATUS_APPLIED)

        validate_node(event["entity_type"], event["properties"])
        self.conn.execute(
            """
            INSERT INTO nodes(node_id, node_type, external_id, properties_json, entity_version, deleted_at_version)
            VALUES (?, ?, ?, ?, ?, NULL)
            ON CONFLICT(node_id) DO UPDATE SET
                properties_json = excluded.properties_json,
                entity_version = excluded.entity_version,
                deleted_at_version = NULL
            """,
            (
                record_id,
                event["entity_type"],
                event["entity_id"],
                json.dumps(event["properties"], sort_keys=True),
                event["version"],
            ),
        )
        return MaterializeResult(True, STATUS_APPLIED)

    def _materialize_edge(self, event: dict[str, Any], allow_equal_version: bool) -> MaterializeResult:
        record_id = edge_key(event["entity_type"], event["entity_id"])
        current = self.conn.execute(
            """
            SELECT entity_version, source_node_id, target_node_id, properties_json
            FROM edges
            WHERE edge_id = ?
            """,
            (record_id,),
        ).fetchone()
        if current:
            if current["entity_version"] > event["version"]:
                return MaterializeResult(False, STATUS_SKIPPED, "stale_version")
            if current["entity_version"] == event["version"] and not allow_equal_version:
                return MaterializeResult(False, STATUS_SKIPPED, "stale_version")

        if event["op"] == "delete":
            source_id = current["source_node_id"] if current else ""
            target_id = current["target_node_id"] if current else ""
            properties_json = current["properties_json"] if current else "{}"
            self.conn.execute(
                """
                INSERT INTO edges(
                    edge_id,
                    edge_type,
                    external_id,
                    source_node_id,
                    target_node_id,
                    properties_json,
                    entity_version,
                    deleted_at_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(edge_id) DO UPDATE SET
                    entity_version = excluded.entity_version,
                    deleted_at_version = excluded.deleted_at_version
                """,
                (
                    record_id,
                    event["entity_type"],
                    event["entity_id"],
                    source_id,
                    target_id,
                    properties_json,
                    event["version"],
                    event["version"],
                ),
            )
            return MaterializeResult(True, STATUS_APPLIED)

        source = event["source"]
        target = event["target"]
        source_id = node_key(source["type"], source["id"])
        target_id = node_key(target["type"], target["id"])
        validate_edge(event["entity_type"], source["type"], target["type"], event["properties"])
        missing = self._missing_live_endpoint(source_id, target_id)

        if not (current and current["entity_version"] == event["version"] and allow_equal_version):
            self.conn.execute(
                """
                INSERT INTO edges(
                    edge_id,
                    edge_type,
                    external_id,
                    source_node_id,
                    target_node_id,
                    properties_json,
                    entity_version,
                    deleted_at_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(edge_id) DO UPDATE SET
                    source_node_id = excluded.source_node_id,
                    target_node_id = excluded.target_node_id,
                    properties_json = excluded.properties_json,
                    entity_version = excluded.entity_version,
                    deleted_at_version = NULL
                """,
                (
                    record_id,
                    event["entity_type"],
                    event["entity_id"],
                    source_id,
                    target_id,
                    json.dumps(event["properties"], sort_keys=True),
                    event["version"],
                ),
            )

        if missing:
            return MaterializeResult(True, STATUS_PENDING, PENDING_REASON_PREFIX, missing)
        return MaterializeResult(True, STATUS_APPLIED)

    def _missing_live_endpoint(self, source_id: str, target_id: str) -> str | None:
        missing: list[str] = []
        for role, node_id in (("source", source_id), ("target", target_id)):
            row = self.conn.execute(
                "SELECT deleted_at_version FROM nodes WHERE node_id = ?",
                (node_id,),
            ).fetchone()
            if row is None or row["deleted_at_version"] is not None:
                missing.append(f"{role}={node_id}")
        if missing:
            return ", ".join(missing)
        return None

    def _apply_to_index(self, event: dict[str, Any]) -> None:
        if event["entity_kind"] == "node":
            node_id = node_key(event["entity_type"], event["entity_id"])
            if event["op"] == "delete":
                self.graph.remove_node(node_id)
            else:
                self.graph.upsert_node(
                    NodeRecord(
                        node_id=node_id,
                        node_type=event["entity_type"],
                        external_id=event["entity_id"],
                        properties=event["properties"],
                        version=event["version"],
                    )
                )
            return

        edge_id = edge_key(event["entity_type"], event["entity_id"])
        if event["op"] == "delete":
            self.graph.remove_edge(edge_id)
            return
        source_id = node_key(event["source"]["type"], event["source"]["id"])
        target_id = node_key(event["target"]["type"], event["target"]["id"])
        self.graph.upsert_edge(
            EdgeRecord(
                edge_id=edge_id,
                edge_type=event["entity_type"],
                source_node_id=source_id,
                target_node_id=target_id,
                properties=event["properties"],
                version=event["version"],
            )
        )
