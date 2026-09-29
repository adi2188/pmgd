"""Algorithm registry.

The brief asks that adding the next five algorithms not require touching the
core. This is the extension point.

An algorithm is a pure function `(graph, params) -> dict`. It reads the graph
through `neighbors()`, `degree()` and the node/edge maps, and knows nothing
about SQLite, the WAL, the HTTP layer or the schema. Registering one is a
decorator; the HTTP surface (`GET /algorithms`, `POST /query/<name>`) and the
storage facade are both generated from this registry, so a new algorithm is a
new function in this file (or any module that imports `register`) and nothing
else changes.

The four below are deliberately of different shapes -- BFS tree walk, induced
subgraph, path reconstruction, whole-component sweep, global ranking -- to show
the interface is not accidentally fitted to shortest path.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from .graph import InMemoryGraph


@dataclass(frozen=True)
class Algorithm:
    name: str
    summary: str
    params: dict
    fn: Callable


_REGISTRY: dict = {}


def register(name: str, summary: str, params: dict) -> Callable:
    def decorate(fn: Callable) -> Callable:
        if name in _REGISTRY:
            raise ValueError(f"algorithm already registered: {name}")
        _REGISTRY[name] = Algorithm(name=name, summary=summary, params=params, fn=fn)
        return fn

    return decorate


def catalog() -> list:
    return [
        {"name": a.name, "summary": a.summary, "params": a.params}
        for a in sorted(_REGISTRY.values(), key=lambda a: a.name)
    ]


def run(graph: InMemoryGraph, name: str, params: dict) -> dict:
    algorithm = _REGISTRY.get(name.replace("-", "_"))
    if algorithm is None:
        raise KeyError(f"unknown algorithm: {name}. known: {sorted(_REGISTRY)}")
    return algorithm.fn(graph, params)


# ----------------------------------------------------------------------
# shared guards
# ----------------------------------------------------------------------

MAX_RESULT_NODES = 50_000


def _require_node(graph: InMemoryGraph, node_id: str, label: str) -> str:
    if node_id not in graph.nodes:
        raise KeyError(f"unknown {label} node: {node_id}")
    return node_id


def _int(params: dict, key: str, default: int, minimum: int = 0) -> int:
    value = int(params.get(key, default))
    if value < minimum:
        raise ValueError(f"{key} must be >= {minimum}")
    return value


# ----------------------------------------------------------------------
# algorithms
# ----------------------------------------------------------------------


@register(
    "neighborhood",
    "Nodes within N hops of a start node, with the induced edge set between them.",
    {
        "start": "node id, required",
        "depth": "int hops, default 1",
        "direction": "out | in | both, default out",
        "edge_types": "optional list of edge types to traverse",
        "node_types": "optional list of node types to admit",
        "induced": "bool, default true -- include edges between result nodes that "
        "BFS did not traverse",
        "limit": f"max nodes, default {MAX_RESULT_NODES}",
    },
)
def neighborhood(graph: InMemoryGraph, params: dict) -> dict:
    start = _require_node(graph, params["start"], "start")
    depth = _int(params, "depth", 1)
    direction = params.get("direction", "out")
    limit = _int(params, "limit", MAX_RESULT_NODES, 1)
    induced = bool(params.get("induced", True))
    allowed_edges = set(params["edge_types"]) if params.get("edge_types") else None
    allowed_nodes = set(params["node_types"]) if params.get("node_types") else None

    seen_nodes = {start}
    traversed = set()
    queue = deque([(start, 0)])
    truncated = False

    while queue:
        node_id, node_depth = queue.popleft()
        if node_depth == depth:
            continue
        for edge_id, neighbor_id in graph.neighbors(node_id, direction):
            edge = graph.edges[edge_id]
            if allowed_edges and edge.edge_type not in allowed_edges:
                continue
            if allowed_nodes and graph.nodes[neighbor_id].node_type not in allowed_nodes:
                continue
            traversed.add(edge_id)
            if neighbor_id not in seen_nodes:
                if len(seen_nodes) >= limit:
                    truncated = True
                    continue
                seen_nodes.add(neighbor_id)
                queue.append((neighbor_id, node_depth + 1))

    edge_ids = traversed
    if induced and not truncated:
        # BFS returns a tree. An agent handed a tree infers a false topology,
        # so close the subgraph: add every edge whose endpoints are both in the
        # result. Costs one pass over the incident edges of the result set.
        edge_ids = set(traversed)
        for node_id in seen_nodes:
            for edge_id in graph.incident_edge_ids(node_id):
                edge = graph.edges[edge_id]
                if allowed_edges and edge.edge_type not in allowed_edges:
                    continue
                if edge.source_node_id in seen_nodes and edge.target_node_id in seen_nodes:
                    edge_ids.add(edge_id)

    return {
        "nodes": [graph.node_json(graph.nodes[n]) for n in sorted(seen_nodes)],
        "edges": [graph.edge_json(graph.edges[e]) for e in sorted(edge_ids)],
        "induced": induced and not truncated,
        "traversed_edge_count": len(traversed),
        "truncated": truncated,
    }


@register(
    "shortest_path",
    "Unweighted shortest path between two nodes (BFS with a parent map).",
    {
        "source": "node id, required",
        "target": "node id, required",
        "direction": "out | in | both, default out",
        "edge_types": "optional list of edge types to traverse",
        "max_depth": "int, default 10",
    },
)
def shortest_path(graph: InMemoryGraph, params: dict) -> dict:
    source = _require_node(graph, params["source"], "source")
    target = _require_node(graph, params["target"], "target")
    direction = params.get("direction", "out")
    max_depth = _int(params, "max_depth", 10)
    allowed_edges = set(params["edge_types"]) if params.get("edge_types") else None

    # A parent map, not per-branch path copies. The previous implementation did
    # `queue.append((n, path_nodes + [n], path_edges + [e]))`, making frontier
    # memory O(V * depth) of list objects; one query on a high-degree node could
    # take down the process that is also the only writer.
    parent = {source: (None, None)}
    depth = {source: 0}
    queue = deque([source])
    found = False

    while queue:
        node_id = queue.popleft()
        if node_id == target:
            found = True
            break
        if depth[node_id] == max_depth:
            continue
        for edge_id, neighbor_id in graph.neighbors(node_id, direction):
            if neighbor_id in parent:
                continue
            if allowed_edges and graph.edges[edge_id].edge_type not in allowed_edges:
                continue
            parent[neighbor_id] = (node_id, edge_id)
            depth[neighbor_id] = depth[node_id] + 1
            queue.append(neighbor_id)

    if not found:
        return {
            "found": False,
            "node_ids": [],
            "edge_ids": [],
            "nodes": [],
            "edges": [],
            "explored_nodes": len(parent),
        }

    node_ids, edge_ids = [], []
    cursor = target
    while cursor is not None:
        node_ids.append(cursor)
        previous, edge_id = parent[cursor]
        if edge_id is not None:
            edge_ids.append(edge_id)
        cursor = previous
    node_ids.reverse()
    edge_ids.reverse()

    return {
        "found": True,
        "node_ids": node_ids,
        "edge_ids": edge_ids,
        "nodes": [graph.node_json(graph.nodes[n]) for n in node_ids],
        "edges": [graph.edge_json(graph.edges[e]) for e in edge_ids],
        "explored_nodes": len(parent),
    }


@register(
    "connected_component",
    "Every node reachable from a start node ignoring edge direction.",
    {
        "start": "node id, required",
        "edge_types": "optional list of edge types to traverse",
        "limit": f"max nodes, default {MAX_RESULT_NODES}",
    },
)
def connected_component(graph: InMemoryGraph, params: dict) -> dict:
    start = _require_node(graph, params["start"], "start")
    limit = _int(params, "limit", MAX_RESULT_NODES, 1)
    allowed_edges = set(params["edge_types"]) if params.get("edge_types") else None

    seen = {start}
    queue = deque([start])
    truncated = False
    while queue:
        node_id = queue.popleft()
        for edge_id, neighbor_id in graph.neighbors(node_id, "both"):
            if allowed_edges and graph.edges[edge_id].edge_type not in allowed_edges:
                continue
            if neighbor_id in seen:
                continue
            if len(seen) >= limit:
                truncated = True
                break
            seen.add(neighbor_id)
            queue.append(neighbor_id)

    by_type: dict = {}
    for node_id in seen:
        node_type = graph.nodes[node_id].node_type
        by_type[node_type] = by_type.get(node_type, 0) + 1
    return {
        "size": len(seen),
        "node_ids": sorted(seen),
        "node_types": by_type,
        "truncated": truncated,
    }


@register(
    "degree_ranking",
    "Top-k nodes by degree, optionally restricted to a node type. Finds the hubs "
    "that make traversal expensive and partitioning hard.",
    {
        "k": "int, default 10",
        "node_type": "optional node type filter",
        "direction": "out | in | both, default both",
    },
)
def degree_ranking(graph: InMemoryGraph, params: dict) -> dict:
    k = _int(params, "k", 10, 1)
    node_type = params.get("node_type")
    direction = params.get("direction", "both")
    if direction not in {"out", "in", "both"}:
        raise ValueError("direction must be one of: out, in, both")

    def score(node_id: str) -> int:
        if direction == "both":
            return graph.degree(node_id)
        table = graph.outgoing if direction == "out" else graph.incoming
        return len(table.get(node_id, ()))

    candidates = (
        n for n, rec in graph.nodes.items() if node_type is None or rec.node_type == node_type
    )
    ranked = sorted(((score(n), n) for n in candidates), key=lambda p: (-p[0], p[1]))[:k]
    return {
        "direction": direction,
        "node_type": node_type,
        "top": [{"id": n, "type": graph.nodes[n].node_type, "degree": d} for d, n in ranked],
    }
