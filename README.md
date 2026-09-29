# Poorgraph

Poorgraph is a small graph service prototype. The demo domain is an ad-tech identity graph: profiles are connected to identifiers like hashed emails, cookies, and device ids, and profiles can belong to audience segments targeted by campaigns.

The point is to show the mechanics of a graph service:

- schema validation
- bulk load
- CDC-style updates
- tabular persistence
- a small WAL/replay story
- in-memory traversal
- a simple algorithm extension point

## Run It

This project uses only the Python standard library.

```bash
python3 -m unittest discover -s tests
python3 -m poorgraph.demo --db /tmp/poorgraph-demo.db
python3 -m poorgraph --db /tmp/poorgraph-demo.db --port 8000
```

Then in another terminal:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/schema
curl http://127.0.0.1:8000/stats
curl http://127.0.0.1:8000/algorithms
```

Load the small demo graph:

```bash
curl -X POST http://127.0.0.1:8000/bulk-load   -H 'Content-Type: application/json'   --data @examples/demo_bulk.json
```

Load the larger identity graph demo:

```bash
curl -X POST http://127.0.0.1:8000/bulk-load   -H 'Content-Type: application/json'   --data @examples/demo_bulk_complex.json
```

Apply a CDC event that quarantines a shared cookie identifier:

```bash
curl -X POST http://127.0.0.1:8000/cdc   -H 'Content-Type: application/json'   --data @examples/cdc_identifier_quarantined.json
```

## Demo Queries

Why is this profile eligible for this campaign?

```bash
curl -X POST http://127.0.0.1:8000/query/shortest-path   -H 'Content-Type: application/json'   --data '{"source":"Profile:p_alice","target":"Campaign:camp_spring_sale","direction":"both","max_depth":4}'
```

What identifiers and segments are near a profile?

```bash
curl -X POST http://127.0.0.1:8000/query/neighborhood   -H 'Content-Type: application/json'   --data '{"start":"Profile:p_alice","depth":1,"direction":"out"}'
```

Which identifiers are hubs?

```bash
curl -X POST http://127.0.0.1:8000/query/degree-ranking   -H 'Content-Type: application/json'   --data '{"k":5,"node_type":"Identifier"}'
```

What cluster is connected through a shared household identifier?

```bash
curl -X POST http://127.0.0.1:8000/query/connected-component   -H 'Content-Type: application/json'   --data '{"start":"Identifier:ctv_living_room"}'
```

## Schema

The schema is in [poorgraph/schema.py](poorgraph/schema.py).

Node types:

- `Profile`
- `Identifier`
- `Segment`
- `Campaign`

Edge types:

- `PROFILE_HAS_IDENTIFIER`: `Profile -> Identifier`
- `PROFILE_IN_SEGMENT`: `Profile -> Segment`
- `CAMPAIGN_TARGETS_SEGMENT`: `Campaign -> Segment`
- `IDENTIFIER_OBSERVED_WITH`: `Identifier -> Identifier`

Ids are strings like `Profile:p_alice` or `Identifier:cookie_alice_web`.

## Storage

SQLite is the system of record. The main tables are:

- `nodes`
- `edges`
- `graph_wal`
- `schema_versions`

Events are written to `graph_wal` before they are materialized into `nodes` or `edges`. On restart, pending WAL rows are replayed.

## Algorithms

Algorithms live in [poorgraph/algorithms.py](poorgraph/algorithms.py). The API route is generic: `/query/<algorithm>`.

Current algorithms:

- `neighborhood`
- `shortest_path`
- `connected_component`
- `degree_ranking`

## Benchmark

```bash
python3 -m poorgraph.bench --quick
```

The benchmark is small. It is there to make scale limits concrete, not to claim this is production-ready.
