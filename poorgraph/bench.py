"""Measured scale envelope.

Every number in WRITEUP.md's scale section comes from this file. Run it:

    python3 -m poorgraph.bench            # slower, broader sizes
    python3 -m poorgraph.bench --quick    # smaller local smoke benchmark

The point is not that these numbers are good. The point is that they exist, so
the claim "this breaks at X" is falsifiable rather than a vibe.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import statistics
import tempfile
import time
import tracemalloc
from typing import Any

from .graph import EdgeRecord, InMemoryGraph, NodeRecord
from .storage import GraphStore

GiB = 1024 ** 3


def _node_items(count: int, prefix: str = "p") -> list[dict[str, Any]]:
    return [
        {
            "type": "Profile",
            "id": f"{prefix}{i}",
            "properties": {
                "status": "active",
                "created_at": "2026-09-01",
                "home_market": "US",
            },
        }
        for i in range(count)
    ]


def _tmp_store(**kwargs) -> GraphStore:
    return GraphStore(os.path.join(tempfile.mkdtemp(), "bench.db"), **kwargs)


# ----------------------------------------------------------------------


def measure_memory(n: int) -> dict[str, float]:
    """Bytes per node and per edge in the live index."""

    def build(nodes: int, edges: int) -> InMemoryGraph:
        g = InMemoryGraph()
        for i in range(nodes):
            g.upsert_node(
                NodeRecord(
                    f"Profile:p{i}",
                    "Profile",
                    f"p{i}",
                    {"status": "active", "created_at": "2026-09-01", "home_market": "US"},
                    1,
                )
            )
        for i in range(edges):
            g.upsert_edge(
                EdgeRecord(
                    f"IDENTITY_LINK:e{i}",
                    "IDENTITY_LINK",
                    f"Profile:p{i % nodes}",
                    f"Profile:p{(i * 7 + 1) % nodes}",
                    {},
                    1,
                )
            )
        return g

    gc.collect()
    tracemalloc.start()
    g1 = build(n, 0)
    nodes_only, _ = tracemalloc.get_traced_memory()
    g2 = build(n, 2 * n)
    both, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    per_node = nodes_only / n
    per_edge = (both - 2 * nodes_only) / (2 * n)
    del g1, g2
    gc.collect()

    per_unit = per_node + 2 * per_edge  # one node plus its two edges
    return {
        "per_node_bytes": round(per_node),
        "per_edge_bytes": round(per_edge),
        "nodes_in_64gib_usable": round(64 * GiB / per_unit),
        "edges_in_64gib_usable": round(2 * 64 * GiB / per_unit),
    }


def measure_single_event_latency(sizes: list[int], repeats: int = 30) -> list[dict[str, Any]]:
    """Per-event CDC cost as the graph grows. Should be flat, not linear."""
    out = []
    for size in sizes:
        store = _tmp_store(durability="relaxed")
        store.bulk_load({"batch_id": f"seed{size}", "nodes": _node_items(size)})
        samples = []
        for k in range(repeats):
            event = {
                "event_id": f"ev-{size}-{k}",
                "op": "upsert",
                "entity_kind": "node",
                "entity_type": "Profile",
                "entity_id": "p0",
                "version": 100 + k,
                "properties": {
                    "status": "active",
                    "created_at": "2026-09-01",
                    "home_market": "US",
                },
            }
            t0 = time.perf_counter()
            store.apply_cdc(event)
            samples.append(time.perf_counter() - t0)
        store.close()
        out.append(
            {
                "graph_nodes": size,
                "p50_ms": round(statistics.median(samples) * 1000, 3),
                "events_per_sec": round(1 / statistics.median(samples)),
            }
        )
    return out


def measure_batch_throughput(bases: list[int], batch: int = 10_000) -> list[dict[str, Any]]:
    out = []
    for base in bases:
        store = _tmp_store(durability="relaxed")
        if base:
            store.bulk_load({"batch_id": "seed", "nodes": _node_items(base, "s")})
        payload = {"batch_id": "live", "nodes": _node_items(batch, "n")}
        t0 = time.perf_counter()
        store.bulk_load(payload)
        dt = time.perf_counter() - t0
        store.close()
        out.append(
            {"existing_nodes": base, "batch": batch, "seconds": round(dt, 3),
             "events_per_sec": round(batch / dt)}
        )
    return out


def measure_durability_cost(events: int = 300) -> dict[str, Any]:
    """What synchronous=FULL actually costs, per commit."""
    result = {}
    for mode in ("strict", "relaxed"):
        store = _tmp_store(durability=mode)
        store.bulk_load({"batch_id": "seed", "nodes": _node_items(1)})
        t0 = time.perf_counter()
        for k in range(events):
            store.apply_cdc(
                {
                    "event_id": f"{mode}-{k}",
                    "op": "upsert",
                    "entity_kind": "node",
                    "entity_type": "Profile",
                    "entity_id": "p0",
                    "version": 10 + k,
                    "properties": {
                        "status": "active",
                        "created_at": "2026-09-01",
                        "home_market": "US",
                    },
                }
            )
        dt = time.perf_counter() - t0
        store.close()
        result[mode] = {"events_per_sec": round(events / dt), "ms_per_event": round(dt / events * 1000, 3)}
    result["fsync_tax"] = round(
        result["relaxed"]["events_per_sec"] / max(result["strict"]["events_per_sec"], 1), 1
    )
    return result


def measure_recovery(sizes: list[int]) -> list[dict[str, Any]]:
    out = []
    for size in sizes:
        path = os.path.join(tempfile.mkdtemp(), "recover.db")
        store = GraphStore(path, durability="relaxed")
        store.bulk_load({"batch_id": "seed", "nodes": _node_items(size)})
        store.close()
        t0 = time.perf_counter()
        reopened = GraphStore(path, durability="relaxed")
        dt = time.perf_counter() - t0
        rows = reopened.graph.stats()["nodes"]
        reopened.close()
        out.append({"rows": rows, "seconds": round(dt, 3), "rows_per_sec": round(rows / dt)})
    return out


def envelope(memory: dict[str, float], recovery: list[dict[str, Any]]) -> dict[str, Any]:
    rate = recovery[-1]["rows_per_sec"]
    nodes = memory["nodes_in_64gib_usable"]
    edges = memory["edges_in_64gib_usable"]
    return {
        "assumed_machine": "single box, 128 GiB RAM, 50% reserved for query working "
        "set, CPython overhead and page cache",
        "graph_ceiling_nodes": nodes,
        "graph_ceiling_edges": edges,
        "cold_start_minutes_at_ceiling": round((nodes + edges) / rate / 60, 1),
        "first_thing_to_break": "cold start time, then query tail latency on hub "
        "nodes; memory is the hard wall behind both",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure the poorgraph scale envelope")
    parser.add_argument("--quick", action="store_true", help="smaller sizes, ~10s")
    parser.add_argument("--json", action="store_true", help="emit raw JSON")
    args = parser.parse_args()

    if args.quick:
        mem_n, sizes, bases, rec = 10_000, [1_000, 5_000, 10_000], [0, 5_000], [5_000]
    else:
        mem_n, sizes, bases, rec = (
            200_000,
            [1_000, 10_000, 50_000, 200_000],
            [0, 50_000, 200_000],
            [25_000, 100_000],
        )

    memory = measure_memory(mem_n)
    latency = measure_single_event_latency(sizes)
    throughput = measure_batch_throughput(bases)
    durability = measure_durability_cost()
    recovery = measure_recovery(rec)
    report = {
        "memory": memory,
        "single_event_latency": latency,
        "batch_throughput": throughput,
        "durability": durability,
        "recovery": recovery,
        "envelope": envelope(memory, recovery),
    }

    if args.json:
        print(json.dumps(report, indent=2))
        return

    print("\n## Memory (measured, tracemalloc; a floor -- excludes arena overhead)\n")
    print(f"  {memory['per_node_bytes']:>6} B/node   (3 small string properties)")
    print(f"  {memory['per_edge_bytes']:>6} B/edge   (no properties)")

    print("\n## Single-event CDC latency vs graph size\n")
    print(f"  {'graph nodes':>12} {'p50':>10} {'events/s':>10}")
    for row in latency:
        print(f"  {row['graph_nodes']:>12,} {row['p50_ms']:>8.2f}ms {row['events_per_sec']:>10,}")

    print("\n## Batched ingest throughput\n")
    print(f"  {'existing nodes':>15} {'batch':>8} {'events/s':>10}")
    for row in throughput:
        print(f"  {row['existing_nodes']:>15,} {row['batch']:>8,} {row['events_per_sec']:>10,}")

    print("\n## Durability cost (single-event commits)\n")
    for mode in ("strict", "relaxed"):
        print(f"  {mode:>8}: {durability[mode]['events_per_sec']:>7,} ev/s "
              f"({durability[mode]['ms_per_event']}ms)")
    print(f"  fsync tax: {durability['fsync_tax']}x")

    print("\n## Cold start (rehydrate + WAL replay)\n")
    print(f"  {'rows':>10} {'seconds':>9} {'rows/s':>10}")
    for row in recovery:
        print(f"  {row['rows']:>10,} {row['seconds']:>8.2f}s {row['rows_per_sec']:>10,}")

    env = report["envelope"]
    print("\n## Derived envelope\n")
    print(f"  assumption: {env['assumed_machine']}")
    print(f"  ceiling   : ~{env['graph_ceiling_nodes']/1e6:.0f}M nodes "
          f"/ ~{env['graph_ceiling_edges']/1e6:.0f}M edges")
    print(f"  cold start at ceiling: ~{env['cold_start_minutes_at_ceiling']} minutes, single threaded")
    print(f"  breaks first: {env['first_thing_to_break']}\n")


if __name__ == "__main__":
    main()
