import json
import tempfile
import unittest
from pathlib import Path

from tools.bench_run import BenchResult, _detect_warnings, _write_summary_json, _write_summary_md


class BenchRunSummaryTests(unittest.TestCase):
    def _result(self) -> BenchResult:
        return BenchResult(
            bench_name="sample_bench",
            mode="B",
            status="PASS",
            failed_gates=[],
            warnings=[],
            ops_run_path="ops_runs/20260216_000000",
            loop_path="ops_runs/20260216_000000/loop_01",
            before={
                "rates": {
                    "bad_domain_mix": 0.10,
                    "unknown_rate": 0.25,
                    "solo_rate": 0.40,
                    "corporate_rate": 0.35,
                },
                "top50": {"top50_effective_good_count": 12},
            },
            after={
                "rates": {
                    "bad_domain_mix": 0.02,
                    "unknown_rate": 0.18,
                    "solo_rate": 0.42,
                    "corporate_rate": 0.38,
                },
                "top50": {"top50_effective_good_count": 12},
            },
            gate_details={
                "before_bad_domain_mix": 0.10,
                "after_bad_domain_mix": 0.02,
            },
            notes="test",
            error="",
        )

    def test_detect_warnings_empty_for_small_changes(self) -> None:
        before = {
            "rates": {
                "unknown_rate": 0.25,
                "solo_rate": 0.40,
                "corporate_rate": 0.35,
            }
        }
        after = {
            "rates": {
                "unknown_rate": 0.18,
                "solo_rate": 0.42,
                "corporate_rate": 0.38,
            }
        }
        warnings = _detect_warnings(before, after)
        self.assertEqual(warnings, [])

    def test_summary_files_written(self) -> None:
        result = self._result()
        with tempfile.TemporaryDirectory() as td:
            report_dir = Path(td)
            _write_summary_md(report_dir, [result])
            _write_summary_json(report_dir, [result])

            md = (report_dir / "BENCH_SUMMARY.md").read_text(encoding="utf-8")
            self.assertIn("sample_bench", md)
            self.assertIn("PASS", md)
            self.assertIn("top50_effective_good_count", md)

            payload = json.loads((report_dir / "BENCH_SUMMARY.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["results"][0]["bench_name"], "sample_bench")
            self.assertEqual(payload["results"][0]["status"], "PASS")


if __name__ == "__main__":
    unittest.main()
