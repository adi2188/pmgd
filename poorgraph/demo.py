"""Demo data and CLI for exercising poorgraph."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .storage import GraphStore


SAMPLE_BULK = {
    "batch_id": "identity-demo-seed",
    "version": 1,
    "nodes": [
        {
            "type": "Profile",
            "id": "p_alice",
            "properties": {
                "status": "active",
                "created_at": "2026-09-01",
                "home_market": "US",
                "confidence": 0.96
            }
        },
        {
            "type": "Profile",
            "id": "p_bob",
            "properties": {
                "status": "active",
                "created_at": "2026-09-01",
                "home_market": "US",
                "confidence": 0.88
            }
        },
        {
            "type": "Identifier",
            "id": "email_alice_hash",
            "properties": {
                "kind": "email_sha256",
                "value_hash": "hash_email_alice_hash",
                "status": "active",
                "first_seen": "2026-09-01"
            }
        },
        {
            "type": "Identifier",
            "id": "cookie_alice_web",
            "properties": {
                "kind": "cookie",
                "value_hash": "hash_cookie_alice_web",
                "status": "active",
                "first_seen": "2026-09-01"
            }
        },
        {
            "type": "Identifier",
            "id": "maid_alice_phone",
            "properties": {
                "kind": "mobile_ad_id",
                "value_hash": "hash_maid_alice_phone",
                "status": "active",
                "first_seen": "2026-09-01"
            }
        },
        {
            "type": "Identifier",
            "id": "cookie_shared_device",
            "properties": {
                "kind": "cookie",
                "value_hash": "hash_cookie_shared_device",
                "status": "review",
                "first_seen": "2026-09-01"
            }
        },
        {
            "type": "Identifier",
            "id": "email_bob_hash",
            "properties": {
                "kind": "email_sha256",
                "value_hash": "hash_email_bob_hash",
                "status": "active",
                "first_seen": "2026-09-01"
            }
        },
        {
            "type": "Identifier",
            "id": "cookie_bob_web",
            "properties": {
                "kind": "cookie",
                "value_hash": "hash_cookie_bob_web",
                "status": "active",
                "first_seen": "2026-09-01"
            }
        },
        {
            "type": "Segment",
            "id": "seg_high_intent",
            "properties": {
                "name": "High Intent Shoppers",
                "category": "intent",
                "size_estimate": 125000
            }
        },
        {
            "type": "Segment",
            "id": "seg_lapsed_buyers",
            "properties": {
                "name": "Lapsed Buyers",
                "category": "retention",
                "size_estimate": 84000
            }
        },
        {
            "type": "Campaign",
            "id": "camp_spring_sale",
            "properties": {
                "name": "Spring Sale",
                "channel": "display",
                "status": "active",
                "budget": 50000
            }
        },
        {
            "type": "Campaign",
            "id": "camp_winback",
            "properties": {
                "name": "Winback",
                "channel": "email",
                "status": "active",
                "budget": 30000
            }
        }
    ],
    "edges": [
        {
            "type": "PROFILE_HAS_IDENTIFIER",
            "id": "p_alice-email",
            "source": {
                "type": "Profile",
                "id": "p_alice"
            },
            "target": {
                "type": "Identifier",
                "id": "email_alice_hash"
            },
            "properties": {
                "evidence": "login",
                "confidence": 0.99,
                "linked_at": "2026-09-02"
            }
        },
        {
            "type": "PROFILE_HAS_IDENTIFIER",
            "id": "p_alice-cookie",
            "source": {
                "type": "Profile",
                "id": "p_alice"
            },
            "target": {
                "type": "Identifier",
                "id": "cookie_alice_web"
            },
            "properties": {
                "evidence": "web_event",
                "confidence": 0.91
            }
        },
        {
            "type": "PROFILE_HAS_IDENTIFIER",
            "id": "p_alice-maid",
            "source": {
                "type": "Profile",
                "id": "p_alice"
            },
            "target": {
                "type": "Identifier",
                "id": "maid_alice_phone"
            },
            "properties": {
                "evidence": "app_login",
                "confidence": 0.95
            }
        },
        {
            "type": "PROFILE_HAS_IDENTIFIER",
            "id": "p_alice-shared",
            "source": {
                "type": "Profile",
                "id": "p_alice"
            },
            "target": {
                "type": "Identifier",
                "id": "cookie_shared_device"
            },
            "properties": {
                "evidence": "household_device",
                "confidence": 0.62
            }
        },
        {
            "type": "PROFILE_HAS_IDENTIFIER",
            "id": "p_bob-email",
            "source": {
                "type": "Profile",
                "id": "p_bob"
            },
            "target": {
                "type": "Identifier",
                "id": "email_bob_hash"
            },
            "properties": {
                "evidence": "login",
                "confidence": 0.98
            }
        },
        {
            "type": "PROFILE_HAS_IDENTIFIER",
            "id": "p_bob-cookie",
            "source": {
                "type": "Profile",
                "id": "p_bob"
            },
            "target": {
                "type": "Identifier",
                "id": "cookie_bob_web"
            },
            "properties": {
                "evidence": "web_event",
                "confidence": 0.9
            }
        },
        {
            "type": "PROFILE_HAS_IDENTIFIER",
            "id": "p_bob-shared",
            "source": {
                "type": "Profile",
                "id": "p_bob"
            },
            "target": {
                "type": "Identifier",
                "id": "cookie_shared_device"
            },
            "properties": {
                "evidence": "household_device",
                "confidence": 0.58
            }
        },
        {
            "type": "PROFILE_IN_SEGMENT",
            "id": "p_alice-high-intent",
            "source": {
                "type": "Profile",
                "id": "p_alice"
            },
            "target": {
                "type": "Segment",
                "id": "seg_high_intent"
            },
            "properties": {
                "score": 0.87,
                "assigned_at": "2026-09-10"
            }
        },
        {
            "type": "PROFILE_IN_SEGMENT",
            "id": "p_bob-lapsed",
            "source": {
                "type": "Profile",
                "id": "p_bob"
            },
            "target": {
                "type": "Segment",
                "id": "seg_lapsed_buyers"
            },
            "properties": {
                "score": 0.76,
                "assigned_at": "2026-09-10"
            }
        },
        {
            "type": "CAMPAIGN_TARGETS_SEGMENT",
            "id": "spring-high-intent",
            "source": {
                "type": "Campaign",
                "id": "camp_spring_sale"
            },
            "target": {
                "type": "Segment",
                "id": "seg_high_intent"
            },
            "properties": {
                "starts_at": "2026-10-01"
            }
        },
        {
            "type": "CAMPAIGN_TARGETS_SEGMENT",
            "id": "winback-lapsed",
            "source": {
                "type": "Campaign",
                "id": "camp_winback"
            },
            "target": {
                "type": "Segment",
                "id": "seg_lapsed_buyers"
            },
            "properties": {
                "starts_at": "2026-10-05"
            }
        },
        {
            "type": "IDENTIFIER_OBSERVED_WITH",
            "id": "cookie-email-alice",
            "source": {
                "type": "Identifier",
                "id": "cookie_alice_web"
            },
            "target": {
                "type": "Identifier",
                "id": "email_alice_hash"
            },
            "properties": {
                "source": "web_login",
                "confidence": 0.94
            }
        },
        {
            "type": "IDENTIFIER_OBSERVED_WITH",
            "id": "cookie-maid-alice",
            "source": {
                "type": "Identifier",
                "id": "cookie_alice_web"
            },
            "target": {
                "type": "Identifier",
                "id": "maid_alice_phone"
            },
            "properties": {
                "source": "app_deeplink",
                "confidence": 0.83
            }
        },
        {
            "type": "IDENTIFIER_OBSERVED_WITH",
            "id": "cookie-email-bob",
            "source": {
                "type": "Identifier",
                "id": "cookie_bob_web"
            },
            "target": {
                "type": "Identifier",
                "id": "email_bob_hash"
            },
            "properties": {
                "source": "web_login",
                "confidence": 0.92
            }
        },
        {
            "type": "IDENTIFIER_OBSERVED_WITH",
            "id": "shared-email-alice",
            "source": {
                "type": "Identifier",
                "id": "cookie_shared_device"
            },
            "target": {
                "type": "Identifier",
                "id": "email_alice_hash"
            },
            "properties": {
                "source": "shared_browser",
                "confidence": 0.55
            }
        },
        {
            "type": "IDENTIFIER_OBSERVED_WITH",
            "id": "shared-email-bob",
            "source": {
                "type": "Identifier",
                "id": "cookie_shared_device"
            },
            "target": {
                "type": "Identifier",
                "id": "email_bob_hash"
            },
            "properties": {
                "source": "shared_browser",
                "confidence": 0.52
            }
        }
    ]
}


CDC_IDENTIFIER_QUARANTINED = {
    "event_id": "identifiers:cookie_shared_device:v2",
    "op": "upsert",
    "entity_kind": "node",
    "entity_type": "Identifier",
    "entity_id": "cookie_shared_device",
    "version": 2,
    "properties": {
        "kind": "cookie",
        "value_hash": "hash_cookie_shared_device",
        "status": "quarantined",
        "first_seen": "2026-09-01"
    }
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Load demo data and run example queries")
    parser.add_argument("--db", default="poorgraph-demo.db", help="SQLite database path")
    args = parser.parse_args()

    db_path = Path(args.db)
    store = GraphStore(db_path)
    try:
        print(json.dumps(store.bulk_load(SAMPLE_BULK), indent=2, sort_keys=True))
        print(json.dumps(store.apply_cdc(CDC_IDENTIFIER_QUARANTINED), indent=2, sort_keys=True))
        print(
            json.dumps(
                store.shortest_path(
                    {
                        "source": "Profile:p_alice",
                        "target": "Campaign:camp_spring_sale",
                        "direction": "both",
                        "max_depth": 4,
                    }
                ),
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        store.close()


if __name__ == "__main__":
    main()
