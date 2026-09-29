"""Demo fixture smoke tests."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from poorgraph.storage import GraphStore


class DemoFixtureTests(unittest.TestCase):
    def test_complex_bulk_fixture_loads_and_exercises_algorithms(self) -> None:
        payload = json.loads(Path("examples/demo_bulk_complex.json").read_text())
        db_path = Path(tempfile.mkdtemp()) / "complex.db"
        store = GraphStore(db_path)
        self.addCleanup(store.close)

        report = store.bulk_load(payload)
        stats = store.stats()["in_memory"]

        self.assertEqual(report["events"], 116)
        self.assertEqual(report["applied"], 116)
        self.assertEqual(stats["nodes"], 49)
        self.assertEqual(stats["edges"], 67)
        self.assertEqual(stats["deferred_edges"], 0)

        household = store.query("connected-component", {"start": "Identifier:ctv_living_room"})
        office = store.query("connected-component", {"start": "Identifier:cookie_office_nat"})
        loyalists = store.query("connected-component", {"start": "Segment:seg_loyalists"})
        self.assertEqual(household["size"], 22)
        self.assertEqual(office["size"], 20)
        self.assertEqual(loyalists["size"], 7)

        path = store.query(
            "shortest-path",
            {
                "source": "Identifier:cookie_jules_web",
                "target": "Campaign:camp_ctv_launch",
                "direction": "both",
                "max_depth": 8,
            },
        )
        self.assertTrue(path["found"])
        self.assertIn("Identifier:ctv_living_room", path["node_ids"])

        ranking = store.query("degree-ranking", {"k": 2, "node_type": "Identifier"})["top"]
        self.assertEqual(
            [item["id"] for item in ranking],
            ["Identifier:cookie_office_nat", "Identifier:ctv_living_room"],
        )


if __name__ == "__main__":
    unittest.main()
