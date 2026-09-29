# Poorgraph Write-Up

## Overview

Poorgraph is a small graph service prototype built around an ad-tech identity graph. The domain is intentionally simple: profiles connect to identifiers such as hashed emails, cookies, mobile ad ids, and household devices. Profiles can also belong to audience segments, and campaigns target those segments.

The goal was not to build a production identity platform. The goal was to show the main pieces of a graph-backed service in a compact codebase: schema validation, ingestion, persistence, replay, in-memory traversal, and a small query layer.

The graph model has four node types:

- `Profile`
- `Identifier`
- `Segment`
- `Campaign`

It has four edge types:

- `PROFILE_HAS_IDENTIFIER`
- `PROFILE_IN_SEGMENT`
- `CAMPAIGN_TARGETS_SEGMENT`
- `IDENTIFIER_OBSERVED_WITH`

That gives enough structure to answer useful demo questions:

- Which identifiers are attached to a profile?
- Why is a profile reachable from a campaign audience?
- Which identifiers have unusually high degree?
- Which profiles are connected through a shared household or device signal?

## Schema

The schema lives in `poorgraph/schema.py`. It defines the allowed node types, edge types, required properties, and valid edge endpoints. For example, `PROFILE_HAS_IDENTIFIER` must connect a `Profile` to an `Identifier`, while `CAMPAIGN_TARGETS_SEGMENT` must connect a `Campaign` to a `Segment`.

The schema is static on purpose. Adding a new node or edge type means editing the schema file and redeploying the service. For this prototype, that is easier to understand and test than a dynamic schema registry.

The validator is strict about types, required fields, and edge endpoints. It is intentionally more flexible about extra properties. Unknown properties are accepted so a source system can add optional metadata without breaking ingestion immediately. That gives the demo a small amount of schema evolution without adding a lot of machinery.

## Ingestion

Bulk load and CDC share the same ingestion path. Bulk load reads a list of nodes and edges from JSON and converts them into events. CDC submits one event at a time. Once an event is created, both paths use the same validation and apply logic.

Each event includes:

- an `event_id`
- an operation
- an entity kind, type, and id
- a version
- a property payload

The storage layer uses the version to make event application idempotent. Duplicate events and older versions are skipped instead of rewriting newer state. Invalid events are recorded with a rejection reason and do not roll back the rest of a bulk load.

Edges are allowed to arrive before their endpoint nodes. In that case, the edge is stored as pending. When the missing nodes arrive later, pending edges are retried and moved into the graph if they become valid. This keeps the ingestion path tolerant of out-of-order source events.

## Storage And Recovery

SQLite is the system of record. The main tables are:

- `nodes`
- `edges`
- `graph_wal`
- `schema_versions`

The write path is deliberately simple:

1. Append the raw event to `graph_wal`.
2. Commit the event.
3. Validate and apply it to `nodes` or `edges`.
4. Mark the WAL row as applied, skipped, pending, or rejected.
5. Update the in-memory graph.

This gives the service a useful recovery behavior without introducing a separate queue or log system. If the process stops after an event is written but before it is fully applied, restart can replay pending WAL rows and rebuild the in-memory graph from SQLite.

SQLite also has its own WAL mode underneath, so the prototype still gets real transactions and durable local storage. That is enough for a small single-process service and keeps the setup easy to run from a fresh checkout.

## In-Memory Graph

Queries run against an in-memory adjacency structure. The graph keeps:

- nodes by id
- edges by id
- outgoing edge ids by source node
- incoming edge ids by target node
- deferred edges waiting for missing endpoints

This representation is easy to reason about and works well for local traversals. The trade-off is memory overhead: Python dictionaries and sets are convenient, but they are not compact.

The quick benchmark is meant to make that trade-off visible. On the test machine, the prototype measured roughly:

- 450 bytes per node with a few small properties
- 611 bytes per edge with no properties

For a larger implementation, I would keep the public API mostly the same but change the internal representation: map string ids to integer ids, store adjacency in compact arrays, and avoid keeping large payloads in memory.

## Data Graph Vs Index Graph

This prototype behaves like a small data graph. It stores node and edge properties locally, loads them into memory, and returns them directly from query results.

That is convenient for a demo because one process has everything it needs.

For a larger identity graph, I would make it more of an index graph. The graph service should own topology, ids, versions, and a few traversal-friendly attributes. Heavier profile attributes, campaign metadata, and source-specific payloads should stay in their owning systems and be fetched separately when needed.

That split keeps graph traversals fast and avoids turning the graph into a second copy of every upstream system.

## Query API

The HTTP API exposes algorithms through `/query/<algorithm>`. Algorithms are registered in `poorgraph/algorithms.py`, which keeps traversal logic separate from storage and ingestion.

The current algorithms are:

- `neighborhood`
- `shortest_path`
- `connected_component`
- `degree_ranking`

These are intentionally basic, but they are enough to demonstrate the identity graph patterns:

- `neighborhood` shows nearby identifiers and segments for a profile.
- `shortest_path` explains how a profile connects to a campaign.
- `connected_component` shows clusters created by shared identifiers.
- `degree_ranking` surfaces hub identifiers that may need review.

Adding another algorithm should mostly mean adding a function and registering it, not changing the storage layer.

## Scale Assumptions

This is a single-machine prototype with modest assumptions:

- one Python process
- one SQLite database
- one writer at a time
- in-memory traversal
- no distributed graph processing

The quick benchmark showed:

- single-event CDC at 10k nodes: about 0.45 ms p50
- batch load: about 9k events/sec on small graphs
- strict SQLite durability: about 3.9k single-event commits/sec
- rehydrate: about 320k rows/sec in the quick test

Those numbers are useful as rough boundaries, not as performance claims. The first bottlenecks would likely be SQLite write throughput, Python object overhead, high-degree identifiers, and restart time as the stored graph grows.

The design is still extensible in the basic ways that matter for this assignment: the schema is centralized, ingestion is event-based, persisted state can be replayed, and algorithms are plugged in behind a small interface.

## What I Would Do Next

If I continued the project, I would keep the current shape and improve the internals:

1. Use integer ids internally instead of string ids in adjacency structures.
2. Store adjacency in compact arrays or typed structures instead of Python sets.
3. Keep only graph-critical fields in memory and hydrate large payloads separately.
4. Add endpoints to inspect pending and rejected events.
5. Add a query for suspicious hubs or low-confidence identity clusters.
6. Add more benchmark cases around high-degree identifiers and replay time.
7. Move to a stronger log and storage setup if ingestion volume became the main problem.

The main value of this prototype is that the graph is real end to end. Events are validated, persisted, replayed, loaded into memory, and queried through a small API.
