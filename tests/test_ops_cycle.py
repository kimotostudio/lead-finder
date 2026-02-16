import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.ops_cycle import (
    _append_after_kpi_delta_report,
    _next_no_progress_streak,
    _merge_stability_gate,
    _merge_val_gate,
    _resolve_val_inputs,
    _run_validation_gate,
    _should_early_stop,
    _should_run_stability,
    compute_progress_theta,
    gate_result,
    should_accept_patch,
)


class OpsCycleTests(unittest.TestCase):
    def test_append_after_kpi_delta_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            report = Path(td) / "AFTER_KPI_REPORT.md"
            report.write_text("# KPI Report\n", encoding="utf-8")

            before = {
                "rates": {
                    "bad_domain_mix": 0.2,
                    "solo_rate": 0.5,
                    "corporate_rate": 0.3,
                    "unknown_rate": 0.2,
                    "city_missing_rate": 0.1,
                },
                "top50": {
                    "top50_good_count": 10,
                    "top50_bad_domain_count": 5,
                    "top50_city_missing_count": 3,
                },
            }
            after = {
                "rates": {
                    "bad_domain_mix": 0.1,
                    "solo_rate": 0.55,
                    "corporate_rate": 0.25,
                    "unknown_rate": 0.2,
                    "city_missing_rate": 0.05,
                },
                "top50": {
                    "top50_good_count": 15,
                    "top50_bad_domain_count": 2,
                    "top50_city_missing_count": 1,
                },
            }

            _append_after_kpi_delta_report(report, before, after)
            text = report.read_text(encoding="utf-8")
            self.assertIn("## BEFORE vs AFTER Delta", text)
            self.assertIn("bad_domain_mix", text)
            self.assertIn("top50_good_count", text)

    def test_gate_allows_top50_good_drop_when_noise_drop_covers_it(self) -> None:
        before = {
            "rates": {
                "bad_domain_mix": 0.04,
                "city_missing_rate": 0.10,
                "solo_rate": 0.50,
                "unknown_rate": 0.15,
            },
            "top50": {
                "top50_good_count": 14,
                "top50_bad_domain_count": 4,
                "top50_effective_good_count": 13,
            },
        }
        after = {
            "rates": {
                "bad_domain_mix": 0.00,
                "city_missing_rate": 0.10,
                "solo_rate": 0.50,
                "unknown_rate": 0.15,
            },
            "top50": {
                "top50_good_count": 13,
                "top50_bad_domain_count": 3,
                "top50_effective_good_count": 13,
            },
        }
        passed, mandatory, _advisory, details = gate_result(before, after, unknown_rate_max=0.20)
        self.assertTrue(passed)
        self.assertTrue(mandatory["top50_good_drop_explained_by_noise_removed"])
        self.assertEqual(details["top50_good_drop"], 1)
        self.assertEqual(details["top50_bad_domain_drop"], 1)
        self.assertTrue(details["top50_good_drop_explained"])

    def test_stability_run_triggered_only_after_small_slice_pass(self) -> None:
        self.assertTrue(_should_run_stability(True, True))
        self.assertFalse(_should_run_stability(False, True))
        self.assertFalse(_should_run_stability(True, False))

    def test_stability_fail_sets_stability_gate_false(self) -> None:
        base = {"bad_domain_mix_non_increasing": True}
        merged = _merge_stability_gate(base, stability_enabled=True, stability_passed=False)
        self.assertIn("stability_passed", merged)
        self.assertFalse(merged["stability_passed"])

    def test_stability_pass_keeps_applied_gate_true(self) -> None:
        base = {"bad_domain_mix_non_increasing": True}
        merged = _merge_stability_gate(base, stability_enabled=True, stability_passed=True)
        self.assertTrue(merged["stability_passed"])
        self.assertTrue(all(merged.values()))

    def test_early_stop_when_small_and_stability_pass(self) -> None:
        self.assertTrue(_should_early_stop(True, True))
        self.assertFalse(_should_early_stop(True, False))
        self.assertFalse(_should_early_stop(False, True))

    def test_theta_calculation_is_deterministic(self) -> None:
        kpi = {
            "rates": {
                "bad_domain_mix": 0.08,
                "unknown_rate": 0.12,
                "solo_rate": 0.61,
                "corporate_rate": 0.19,
            },
            "top50": {
                "top50_effective_good_count": 17,
            },
        }
        t1 = compute_progress_theta(kpi)
        t2 = compute_progress_theta(kpi)
        self.assertEqual(t1["theta"], t2["theta"])
        self.assertEqual(t1["components"], t2["components"])

    def test_accept_reject_logic_requires_gates_and_theta_improvement(self) -> None:
        accepted, improved = should_accept_patch(
            mandatory_gates={"bad_domain_mix_non_increasing": True, "stability_passed": True},
            theta_before=0.70,
            theta_after=0.71,
        )
        self.assertTrue(improved)
        self.assertTrue(accepted)

        rejected_same_theta, improved_same = should_accept_patch(
            mandatory_gates={"bad_domain_mix_non_increasing": True, "stability_passed": True},
            theta_before=0.70,
            theta_after=0.70,
        )
        self.assertFalse(improved_same)
        self.assertFalse(rejected_same_theta)

        rejected_bad_gate, improved2 = should_accept_patch(
            mandatory_gates={"bad_domain_mix_non_increasing": False, "stability_passed": True},
            theta_before=0.70,
            theta_after=0.71,
        )
        self.assertTrue(improved2)
        self.assertFalse(rejected_bad_gate)

    def test_no_progress_streak_resets_only_when_theta_improves(self) -> None:
        streak = _next_no_progress_streak(theta_has_improved=False, prev_streak=2)
        self.assertEqual(streak, 3)
        reset = _next_no_progress_streak(theta_has_improved=True, prev_streak=3)
        self.assertEqual(reset, 0)

    def test_resolve_val_inputs_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            manifest = Path(td) / "vals.txt"
            manifest.write_text("# comment\n/a.csv\n\n/b.csv\n", encoding="utf-8")
            vals = _resolve_val_inputs(["/a.csv"], str(manifest))
            self.assertEqual([str(p) for p in vals], ["/a.csv", "/b.csv"])

    def test_run_validation_gate_generates_artifacts_and_fails_on_one_val(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            val1 = run_dir / "v1.csv"
            val2 = run_dir / "v2.csv"
            csv_text = "URL,title,店舗名,visible_text,reasons\nhttps://example.com,a,a,t,r\n"
            val1.write_text(csv_text, encoding="utf-8")
            val2.write_text(csv_text, encoding="utf-8")
            log_path = run_dir / "RUN.log"
            log_path.write_text("", encoding="utf-8")

            kpi_seq = [
                {"rates": {"bad_domain_mix": 0.10, "unknown_rate": 0.10}, "top50": {"top50_effective_good_count": 10}},
                {"rates": {"bad_domain_mix": 0.05, "unknown_rate": 0.10}, "top50": {"top50_effective_good_count": 10}},
                {"rates": {"bad_domain_mix": 0.10, "unknown_rate": 0.10}, "top50": {"top50_effective_good_count": 10}},
                {"rates": {"bad_domain_mix": 0.20, "unknown_rate": 0.10}, "top50": {"top50_effective_good_count": 10}},
            ]

            with patch("tools.ops_cycle.kpi_generate.run", side_effect=kpi_seq):
                payload = _run_validation_gate(
                    run_dir=run_dir,
                    val_inputs=[val1, val2],
                    input_slice_n=10,
                    theta_snapshot={"gate_policy": {}},
                    theta_hash="abc123",
                    unknown_rate_max=0.20,
                    log_path=log_path,
                )

            self.assertFalse(payload["all_passed"])
            self.assertTrue((run_dir / "VAL_SUMMARY.md").exists())
            self.assertTrue((run_dir / "VAL_ACTIONS.json").exists())
            self.assertEqual(len(payload["results"]), 2)
            self.assertFalse(payload["results"][1]["passed"])

    def test_merge_val_gate_marks_failure(self) -> None:
        merged = _merge_val_gate({"bad_domain_mix_non_increasing": True}, val_enabled=True, val_passed=False)
        self.assertFalse(merged["val_passed"])


if __name__ == "__main__":
    unittest.main()
