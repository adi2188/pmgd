"""Domain schema and validation for the demo graph.

The chosen domain is a small ad-tech identity graph. It stitches profiles to
identifiers such as hashed emails, cookies, and device ids, then connects
profiles to audience segments and campaigns.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any


SCHEMA_VERSION = 1

# Unknown properties are accepted so producers can add optional fields before
# this service is redeployed. Required fields and edge endpoints are still
# validated.
STRICT_UNKNOWN_PROPERTIES = False

NODE_TYPES: dict[str, dict[str, Any]] = {
    "Profile": {
        "required": {"status": "str", "created_at": "str"},
        "optional": {"home_market": "str", "confidence": "number"},
    },
    "Identifier": {
        "required": {"kind": "str", "value_hash": "str", "status": "str"},
        "optional": {"first_seen": "str"},
    },
    "Segment": {
        "required": {"name": "str", "category": "str"},
        "optional": {"size_estimate": "number"},
    },
    "Campaign": {
        "required": {"name": "str", "channel": "str", "status": "str"},
        "optional": {"budget": "number"},
    },
}

EDGE_TYPES: dict[str, dict[str, Any]] = {
    "PROFILE_HAS_IDENTIFIER": {
        "source": "Profile",
        "target": "Identifier",
        "required": {},
        "optional": {"evidence": "str", "confidence": "number", "linked_at": "str"},
    },
    "PROFILE_IN_SEGMENT": {
        "source": "Profile",
        "target": "Segment",
        "required": {},
        "optional": {"score": "number", "assigned_at": "str"},
    },
    "CAMPAIGN_TARGETS_SEGMENT": {
        "source": "Campaign",
        "target": "Segment",
        "required": {},
        "optional": {"starts_at": "str"},
    },
    "IDENTIFIER_OBSERVED_WITH": {
        "source": "Identifier",
        "target": "Identifier",
        "required": {},
        "optional": {"source": "str", "confidence": "number"},
    },
}


class SchemaError(ValueError):
    """Raised when incoming graph data violates the declared schema."""


def node_key(node_type: str, external_id: str) -> str:
    if not node_type or ":" in node_type:
        raise SchemaError("node type must be non-empty and cannot contain ':'")
    if not external_id or ":" in external_id:
        raise SchemaError("node external id must be non-empty and cannot contain ':'")
    return f"{node_type}:{external_id}"


def edge_key(edge_type: str, external_id: str) -> str:
    if not edge_type or ":" in edge_type:
        raise SchemaError("edge type must be non-empty and cannot contain ':'")
    if not external_id or ":" in external_id:
        raise SchemaError("edge external id must be non-empty and cannot contain ':'")
    return f"{edge_type}:{external_id}"


def schema_as_dict() -> dict[str, Any]:
    return {
        "version": SCHEMA_VERSION,
        "fingerprint": schema_fingerprint(),
        "node_types": deepcopy(NODE_TYPES),
        "edge_types": deepcopy(EDGE_TYPES),
        "evolution_policy": {
            "compatible": [
                "add optional property",
                "add new node type",
                "add new edge type",
            ],
            "requires_new_version": [
                "rename or remove type",
                "make optional property required",
                "change edge endpoints",
            ],
        },
    }


def schema_fingerprint() -> str:
    payload = json.dumps(
        {"node_types": NODE_TYPES, "edge_types": EDGE_TYPES}, sort_keys=True
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def validate_node(node_type: str, properties: dict[str, Any]) -> None:
    if node_type not in NODE_TYPES:
        raise SchemaError(f"unknown node type: {node_type}")
    _validate_properties(f"node {node_type}", NODE_TYPES[node_type], properties)


def validate_edge(
    edge_type: str,
    source_type: str,
    target_type: str,
    properties: dict[str, Any],
) -> None:
    if edge_type not in EDGE_TYPES:
        raise SchemaError(f"unknown edge type: {edge_type}")
    edge_def = EDGE_TYPES[edge_type]
    if edge_def["source"] != source_type or edge_def["target"] != target_type:
        raise SchemaError(
            f"edge {edge_type} expects {edge_def['source']} -> {edge_def['target']}, "
            f"got {source_type} -> {target_type}"
        )
    _validate_properties(f"edge {edge_type}", edge_def, properties)


def _validate_properties(label: str, definition: dict[str, Any], properties: dict[str, Any]) -> None:
    if not isinstance(properties, dict):
        raise SchemaError(f"{label} properties must be an object")

    allowed = set(definition["required"]) | set(definition["optional"])
    missing = set(definition["required"]) - set(properties)
    unknown = set(properties) - allowed

    if missing:
        raise SchemaError(f"{label} missing required properties: {sorted(missing)}")
    if unknown and STRICT_UNKNOWN_PROPERTIES:
        raise SchemaError(f"{label} has unknown properties: {sorted(unknown)}")

    type_specs = definition["required"] | definition["optional"]
    for key, value in properties.items():
        if key in unknown:
            continue
        expected = type_specs[key]
        if not _matches_type(value, expected):
            raise SchemaError(f"{label}.{key} must be {expected}, got {type(value).__name__}")


def _matches_type(value: Any, expected: str) -> bool:
    if expected == "str":
        return isinstance(value, str)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "bool":
        return isinstance(value, bool)
    raise SchemaError(f"unsupported schema type: {expected}")
