#!/usr/bin/env python3
"""
OPS daily loop runner (precision-first, minimal diffs).

Flow:
1) Slice input CSV deterministically (first N rows)
2) Generate BEFORE KPI/REPORT
3) Propose/apply minimal precision patch
4) Generate AFTER KPI/REPORT
5) Gate on precision KPIs and revert if gates fail
6) Emit run artifacts under ops_runs/<timestamp>/
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from tools import kpi_generate
from tools.patch_engine import PatchPlan, PatchResult, apply_patch, propose_patch, revert_patch


JST = ZoneInfo("Asia/Tokyo")
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "web_app" / "output" / "merge_fukuoka_all_queries.csv"
OPS_RUNS_DIR = ROOT / "ops_runs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one OPS precision-first cycle.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input merged CSV path")
    parser.add_argument("--slice", type=int, default=200, help="Deterministic safe slice size (first N rows)")
    return parser.parse_args()


def _ts() -> str:
    return datetime.now(JST).strftime("%Y%m%d_%H%M%S")


def _log(line: str, log_path: Path) -> None:
    timestamp = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    text = f"[{timestamp}] {line}"
    print(text)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(text + "\n")


def copy_slice(input_csv: Path, out_csv: Path, n: int) -> int:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with input_csv.open("r", encoding="utf-8-sig", newline="") as rf:
        reader = csv.DictReader(rf)
        fieldnames = reader.fieldnames or []
        rows = []
        for i, row in enumerate(reader):
            if i >= n:
                break
            rows.append(row)

    with out_csv.open("w", encoding="utf-8-sig", newline="") as wf:
        writer = csv.DictWriter(wf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def gate_result(before: dict[str, Any], after: dict[str, Any]) -> tuple[bool, dict[str, bool], dict[str, Any]]:
    before_bad = float(before.get("rates", {}).get("bad_domain_mix", 1.0))
    after_bad = float(after.get("rates", {}).get("bad_domain_mix", 1.0))
    before_top50 = int(before.get("top50", {}).get("top50_good_count", 0))
    after_top50 = int(after.get("top50", {}).get("top50_good_count", 0))

    gates = {
        "bad_domain_mix_non_increasing": after_bad <= before_bad,
        "top50_good_count_non_decreasing": after_top50 >= before_top50,
    }
    passed = all(gates.values())
    details = {
        "before_bad_domain_mix": before_bad,
        "after_bad_domain_mix": after_bad,
        "before_top50_good_count": before_top50,
        "after_top50_good_count": after_top50,
    }
    return passed, gates, details


def write_summary(
    summary_path: Path,
    before: dict[str, Any],
    after: dict[str, Any],
    patch_plan: PatchPlan,
    patch_result: PatchResult,
    gates_passed: bool,
    gates: dict[str, bool],
    gate_details: dict[str, Any],
    reverted: bool,
) -> None:
    lines = [
        "# OPS Cycle Summary",
        "",
        f"- target_file: `{patch_plan.target_file}`",
        f"- candidate_domains: `{', '.join(patch_plan.candidate_domains) if patch_plan.candidate_domains else '(none)'}`",
        f"- patch_applied: `{patch_result.applied}`",
        f"- patch_message: `{patch_result.message}`",
        f"- gates_passed: `{gates_passed}`",
        f"- reverted: `{reverted}`",
        "",
        "## KPI Before -> After",
        "",
        f"- bad_domain_mix: `{gate_details['before_bad_domain_mix']}` -> `{gate_details['after_bad_domain_mix']}`",
        f"- top50_good_count: `{gate_details['before_top50_good_count']}` -> `{gate_details['after_top50_good_count']}`",
        "",
        "## Gate Checks",
        "",
    ]
    for key, value in gates.items():
        lines.append(f"- {key}: `{value}`")

    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_cycle(input_path: Path, slice_n: int) -> int:
    run_dir = OPS_RUNS_DIR / _ts()
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "RUN.log"

    _log("OPS cycle started", log_path)
    _log(f"input_csv={input_path}", log_path)
    _log(f"slice={slice_n}", log_path)

    if not input_path.exists():
        _log(f"ERROR: input CSV not found: {input_path}", log_path)
        return 1

    slice_csv = run_dir / "slice_input.csv"
    slice_rows = copy_slice(input_path, slice_csv, slice_n)
    _log(f"slice rows written: {slice_rows} -> {slice_csv}", log_path)

    before_json = run_dir / "BEFORE_KPI.json"
    before_md = run_dir / "BEFORE_KPI_REPORT.md"
    before = kpi_generate.run(slice_csv, before_json, before_md, slice_value=None)
    _log(f"before KPI generated: {before_json}", log_path)

    patch_plan = propose_patch(ROOT, before, max_domains=5)
    patch_result = apply_patch(patch_plan, run_dir)
    patch_diff_path = run_dir / "PATCH.diff"
    patch_diff_path.write_text(patch_result.diff_text, encoding="utf-8")
    _log(
        f"patch applied={patch_result.applied} message={patch_result.message} "
        f"domains={patch_result.domains_added}",
        log_path,
    )

    after_json = run_dir / "AFTER_KPI.json"
    after_md = run_dir / "AFTER_KPI_REPORT.md"
    after = kpi_generate.run(slice_csv, after_json, after_md, slice_value=None)
    _log(f"after KPI generated: {after_json}", log_path)

    passed, gates, gate_details = gate_result(before, after)
    reverted = False
    if not passed:
        reverted = revert_patch(patch_result)
        _log(f"gates failed -> reverted={reverted}", log_path)
    else:
        _log("gates passed -> patch kept", log_path)

    actions = {
        "run_dir": str(run_dir),
        "input_csv": str(input_path),
        "slice_csv": str(slice_csv),
        "before_kpi": str(before_json),
        "after_kpi": str(after_json),
        "before_report": str(before_md),
        "after_report": str(after_md),
        "patch_diff": str(patch_diff_path),
        "patch": {
            "target_file": str(patch_plan.target_file),
            "domains": patch_plan.candidate_domains,
            "applied": patch_result.applied,
            "message": patch_result.message,
            "backup_file": str(patch_result.backup_file) if patch_result.backup_file else None,
        },
        "gates": gates,
        "gate_details": gate_details,
        "passed": passed,
        "reverted": reverted,
    }
    (run_dir / "ACTIONS.json").write_text(
        json.dumps(actions, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    write_summary(
        summary_path=run_dir / "SUMMARY.md",
        before=before,
        after=after,
        patch_plan=patch_plan,
        patch_result=patch_result,
        gates_passed=passed,
        gates=gates,
        gate_details=gate_details,
        reverted=reverted,
    )
    _log("OPS cycle completed", log_path)
    _log(f"artifacts: {run_dir}", log_path)
    return 0


def main() -> int:
    args = parse_args()
    if args.slice <= 0:
        print("ERROR: --slice must be > 0")
        return 2
    input_path = Path(args.input)
    return run_cycle(input_path=input_path, slice_n=args.slice)


if __name__ == "__main__":
    raise SystemExit(main())

