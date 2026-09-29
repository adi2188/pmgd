"""In-memory graph index.

This is a *rebuildable index*, never the system of record. Everything here can
be reconstructed from the `nodes` and `edges` tables in SQLite.

Two properties matter and are easy to get wrong:

1.  Mutation is incremental. An earlier version of this service rebuilt the
    whole index after every write, which made single-event ingest O(V+E) and
    total ingest O(N*(V+E)). It also held two full copies of the graph alive
    during the swap, so the real memory ceiling was half of RAM and the process
    died on a *write*. See `poorgraph/bench.py` for the measurements.

2.  Edge liveness is *derived*, not stored. An edge is live iff it is not
    deleted AND both of its endpoints are live. Edges that fail that test are
    held in a deferred set rather than dropped. That single decision buys three
    things: out-of-order CDC (edge before node) self-heals, delete-then-recreate
    of a node restores its edges, and we never silently lose an event.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Iterator


_NO_EDGES: frozenset = frozenset()


@dataclass(frozen=True)
class NodeRecord:
    __slots__ = ("node_id", "node_type", "external_id", "properties", "version")
    node_id: str
    node_type: str
    external_id: str
    properties: dict[str, Any]
    version: int


@dataclass(frozen=True)
class EdgeRecord:
    __slots__ = (
        "edge_id",
        "edge_type",
        "source_node_id",
        "target_node_id",
        "properties",
        "version",
    )
    edge_id: str
    edge_type: str
    source_node_id: str
    target_node_id: str
    properties: dict[str, Any]
    version: int


class InMemoryGraph:
    """Adjacency-list index with incremental mutation and deferred edges.

    Adjacency sets are created lazily, on first incident edge, so an isolated
    node costs one dict slot rather than two empty sets (216 bytes each in
    CPython).
    """

    def __init__(self) -> None:
        self.nodes: dict[str, NodeRecord] = {}
        self.edges: dict[str, EdgeRecord] = {}
        self.outgoing: dict[str, set] = {}
        self.incoming: dict[str, set] = {}
        # Edges whose endpoints are not (yet) both live. Kept, not dropped.
        self._deferred: dict[str, EdgeRecord] = {}
        self._deferred_by_node: dict[str, set] = {}

    # ------------------------------------------------------------------
    # mutation
    # ------------------------------------------------------------------

    def upsert_node(self, node: NodeRecord) -> None:
        was_absent = node.node_id not in self.nodes
        self.nodes[node.node_id] = node
        if was_absent:
            for edge_id in list(self._deferred_by_node.get(node.node_id, ())):
                self._try_promote(edge_id)

    def remove_node(self, node_id: str) -> None:
        """Mark a node not-live. Incident edges are deferred, never discarded."""
        if node_id not in self.nodes:
            return
        del self.nodes[node_id]
        incident = set(self.outgoing.get(node_id, _NO_EDGES))
        incident |= set(self.incoming.get(node_id, _NO_EDGES))
        for edge_id in incident:
            self._demote(edge_id)
        self.outgoing.pop(node_id, None)
        self.incoming.pop(node_id, None)

    def upsert_edge(self, edge: EdgeRecord) -> None:
        # Endpoints can change on an update, so clear any previous placement.
        self.remove_edge(edge.edge_id)
        if edge.source_node_id in self.nodes and edge.target_node_id in self.nodes:
            self._install(edge)
        else:
            self._defer(edge)

    def remove_edge(self, edge_id: str) -> None:
        """Hard removal: an explicit edge delete, not an endpoint going away."""
        edge = self.edges.pop(edge_id, None)
        if edge is not None:
            self._unlink(edge)
        deferred = self._deferred.pop(edge_id, None)
        if deferred is not None:
            self._unindex_deferred(deferred)

    # ------------------------------------------------------------------
    # internal placement helpers
    # ------------------------------------------------------------------

    def _install(self, edge: EdgeRecord) -> None:
        self.edges[edge.edge_id] = edge
        self.outgoing.setdefault(edge.source_node_id, set()).add(edge.edge_id)
        self.incoming.setdefault(edge.target_node_id, set()).add(edge.edge_id)

    def _unlink(self, edge: EdgeRecord) -> None:
        out = self.outgoing.get(edge.source_node_id)
        if out is not None:
            out.discard(edge.edge_id)
            if not out:
                del self.outgoing[edge.source_node_id]
        inc = self.incoming.get(edge.target_node_id)
        if inc is not None:
            inc.discard(edge.edge_id)
            if not inc:
                del self.incoming[edge.target_node_id]

    def _defer(self, edge: EdgeRecord) -> None:
        self._deferred[edge.edge_id] = edge
        for node_id in (edge.source_node_id, edge.target_node_id):
            if node_id not in self.nodes:
                self._deferred_by_node.setdefault(node_id, set()).add(edge.edge_id)

    def _unindex_deferred(self, edge: EdgeRecord) -> None:
        for node_id in (edge.source_node_id, edge.target_node_id):
            bucket = self._deferred_by_node.get(node_id)
            if bucket is not None:
                bucket.discard(edge.edge_id)
                if not bucket:
                    del self._deferred_by_node[node_id]

    def _demote(self, edge_id: str) -> None:
        edge = self.edges.pop(edge_id, None)
        if edge is None:
            return
        self._unlink(edge)
        self._defer(edge)

    def _try_promote(self, edge_id: str) -> None:
        edge = self._deferred.get(edge_id)
        if edge is None:
            return
        if edge.source_node_id in self.nodes and edge.target_node_id in self.nodes:
            del self._deferred[edge_id]
            self._unindex_deferred(edge)
            self._install(edge)

    # ------------------------------------------------------------------
    # read side
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, int]:
        return {
            "nodes": len(self.nodes),
            "edges": len(self.edges),
            "deferred_edges": len(self._deferred),
            "nodes_awaited_by_deferred_edges": len(self._deferred_by_node),
            "adjacency_lists": len(self.outgoing) + len(self.incoming),
        }

    def deferred_edge_sample(self, limit: int = 20) -> list:
        """Operational escape hatch: which edges are waiting on which nodes."""
        out = []
        for edge_id, edge in list(self._deferred.items())[:limit]:
            missing = [
                n for n in (edge.source_node_id, edge.target_node_id) if n not in self.nodes
            ]
            out.append({"edge_id": edge_id, "type": edge.edge_type, "awaiting": missing})
        return out

    def neighbors(self, node_id: str, direction: str) -> Iterator:
        """Yield (edge_id, neighbor_node_id). The single traversal primitive."""
        if direction not in {"out", "in", "both"}:
            raise ValueError("direction must be one of: out, in, both")
        if direction in {"out", "both"}:
            for edge_id in self.outgoing.get(node_id, _NO_EDGES):
                yield edge_id, self.edges[edge_id].target_node_id
        if direction in {"in", "both"}:
            for edge_id in self.incoming.get(node_id, _NO_EDGES):
                yield edge_id, self.edges[edge_id].source_node_id

    def incident_edge_ids(self, node_id: str) -> Iterator:
        for edge_id in self.outgoing.get(node_id, _NO_EDGES):
            yield edge_id
        for edge_id in self.incoming.get(node_id, _NO_EDGES):
            yield edge_id

    def degree(self, node_id: str) -> int:
        return len(self.outgoing.get(node_id, _NO_EDGES)) + len(
            self.incoming.get(node_id, _NO_EDGES)
        )

    @staticmethod
    def node_json(node: NodeRecord) -> dict:
        return {
            "id": node.node_id,
            "type": node.node_type,
            "external_id": node.external_id,
            "properties": node.properties,
            "version": node.version,
        }

    @staticmethod
    def edge_json(edge: EdgeRecord) -> dict:
        return {
            "id": edge.edge_id,
            "type": edge.edge_type,
            "source": edge.source_node_id,
            "target": edge.target_node_id,
            "properties": edge.properties,
            "version": edge.version,
        }

    def bulk_install(self, nodes: Iterable, edges: Iterable) -> None:
        """Rehydration fast path: nodes first, then edges, so promotion is rare."""
        for node in nodes:
            self.nodes[node.node_id] = node
        for edge in edges:
            self.upsert_edge(edge)
