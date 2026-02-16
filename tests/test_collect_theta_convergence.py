import importlib.util
from pathlib import Path
import sys
import unittest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "collect_theta_convergence.py"
spec = importlib.util.spec_from_file_location("collect_theta_convergence", SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class CollectThetaConvergenceTests(unittest.TestCase):
    def test_parse_loop_summary_text_extracts_metrics(self) -> None:
        text = """# LOOP_SUMMARY

## loop_01
- run_dir: `/tmp/ops_runs/20260216_000000/loop_01`
- passed: `True`
- selected_candidate: `B:商工会`
- theta: `0.600000` -> `0.650000` improved=`True`
- bad_domain_mix: `0.050000` -> `0.020000`
- top50_effective_good_count: `11` -> `13`
- after_unknown_rate: `0.100000` (max `0.200000`)
- stability: ran=`True` passed=`True` report=`/tmp/report.md`
"""
        loops = mod.parse_loop_summary_text(text)
        self.assertEqual(len(loops), 1)
        row = loops[0]
        self.assertEqual(row.loop_id, "loop_01")
        self.assertTrue(row.passed)
        self.assertEqual(row.selected_candidate, "B:商工会")
        self.assertAlmostEqual(row.theta_before, 0.6)
        self.assertAlmostEqual(row.theta_after, 0.65)
        self.assertTrue(row.theta_improved)
        self.assertEqual(row.top50_eff_before, 11)
        self.assertEqual(row.top50_eff_after, 13)
        self.assertAlmostEqual(row.unknown_after, 0.1)
        self.assertAlmostEqual(row.unknown_max, 0.2)
        self.assertTrue(row.stability_ran)
        self.assertTrue(row.stability_passed)


if __name__ == "__main__":
    unittest.main()
