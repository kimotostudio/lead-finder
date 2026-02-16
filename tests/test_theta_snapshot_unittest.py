import json
import unittest

from tools.theta import get_theta_snapshot


class ThetaSnapshotTests(unittest.TestCase):
    def test_theta_snapshot_is_json_serializable_and_stable(self) -> None:
        snap1 = get_theta_snapshot()
        snap2 = get_theta_snapshot()
        self.assertEqual(snap1, snap2)
        payload = json.dumps(snap1, ensure_ascii=False, sort_keys=True)
        self.assertIn("gate_policy", payload)
        self.assertIn("noise_domain_suffixes", payload)


if __name__ == "__main__":
    unittest.main()
