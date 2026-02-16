#!/usr/bin/env python3
"""
Run fixed benchmark suites for ops_cycle and emit pass/fail summaries.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
import time
from zoneinfo import ZoneInfo

from tools import ops_cycle
from tools.bench_config import (
    BENCHES,
    CORPORATE_INCREASE_WARN,
    SOLO_INCREASE_WARN,
    UNKNOWN_DROP_WARN,
    BenchSpec,
)


JST = ZoneInfo("Asia/Tokyo")
ROOT = Path(__file__).resolve().parents[1]
OPS_RUNS_DIR = ROOT / "ops_runs"


@dataclass
class BenchResult:
    bench_name: str
    mode: str
    status: str
    failed_gates: list[str]
    warnings: list[str]
    ops_run_path: str
    loop_path: str
    before: dict[str, Any]
    after: dict[str, Any]
    gate_details: dict[str, Any]
    notes: str
    error: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fixed ops_cycle benchmarks and summarize results.")
    parser.add_argument("--mode", choices=("A", "B", "both"), default="both", help="Benchmark mode")
    parser.add_argument("--slice", type=int, default=200, help="Override slice for all benches")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first FAIL")
    parser.add_argument("--report-dir", default=None, help="Output report dir (default: ops_runs/bench_<ts>)")
    return parser.parse_args()


def _ts() -> str:
    return datetime.now(JST).strftime("%Y%m%d_%H%M%S")


def _list_ops_dirs() -> set[str]:
    if not OPS_RUNS_DIR.exists():
        return set()
    return {p.name for p in OPS_RUNS_DIR.iterdir() if p.is_dir()}


def _new_ops_root(before: set[str], after: set[str]) -> Path | None:
    new_names = sorted(list(after - before))
    if not new_names:
        return None
    return OPS_RUNS_DIR / new_names[-1]


def _latest_ops_root_after(start_ts: float) -> Path | None:
    if not OPS_RUNS_DIR.exists():
        return None
    candidates = [p for p in OPS_RUNS_DIR.iterdir() if p.is_dir() and p.stat().st_mtime >= start_ts]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _first_loop_dir(run_root: Path) -> Path:
    loops = sorted([p for p in run_root.iterdir() if p.is_dir() and p.name.startswith("loop_")])
    if loops:
        return loops[0]
    return run_root


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_section(md_text: str, title: str) -> str:
    lines = md_text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.strip() == title:
            start = i
            break
    if start is None:
        return ""
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("## "):
            end = j
            break
    return "\n".join(lines[start:end]).strip()


def _detect_warnings(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    before_rates = before.get("rates", {})
    after_rates = after.get("rates", {})
    unknown_drop = float(before_rates.get("unknown_rate", 0.0)) - float(after_rates.get("unknown_rate", 0.0))
    corporate_increase = float(after_rates.get("corporate_rate", 0.0)) - float(before_rates.get("corporate_rate", 0.0))
    solo_increase = float(after_rates.get("solo_rate", 0.0)) - float(before_rates.get("solo_rate", 0.0))

    if unknown_drop >= UNKNOWN_DROP_WARN and corporate_increase >= CORPORATE_INCREASE_WARN:
        warnings.append(
            f"unknown_rate dropped by {unknown_drop:.6f} while corporate_rate rose by {corporate_increase:.6f}"
        )
    if solo_increase >= SOLO_INCREASE_WARN:
        warnings.append(f"solo_rate rose sharply by {solo_increase:.6f}")
    return warnings


def _snapshot_sources() -> dict[Path, str]:
    tracked = [
        ROOT / "src" / "filters.py",
        ROOT / "tools" / "kpi_generate.py",
    ]
    snapshot: dict[Path, str] = {}
    for path in tracked:
        if path.exists():
            snapshot[path] = path.read_text(encoding="utf-8")
    return snapshot


def _restore_sources(snapshot: dict[Path, str]) -> None:
    for path, text in snapshot.items():
        path.write_text(text, encoding="utf-8")


def _run_one(spec: BenchSpec, mode: str, slice_override: int) -> BenchResult:
    input_path = Path(spec.input_csv_path)
    if not input_path.exists():
        return BenchResult(
            bench_name=spec.name,
            mode=mode,
            status="FAIL",
            failed_gates=["input_missing"],
            warnings=[],
            ops_run_path="",
            loop_path="",
            before={},
            after={},
            gate_details={},
            notes=spec.notes,
            error=f"input CSV missing: {input_path}",
        )

    before_dirs = _list_ops_dirs()
    start_ts = time.time()
    snapshot = _snapshot_sources()
    try:
        code = ops_cycle.run_cycle(
            input_path=input_path,
            slice_n=slice_override if slice_override > 0 else spec.slice,
            mode=mode,
            loop_n=spec.loop,
            max_candidates=3,
            unknown_rate_max=0.20,
            stability_enabled=True,
            stability_slice_n=200,
            val_inputs=[],
            no_progress_k=3,
        )
    finally:
        _restore_sources(snapshot)
    after_dirs = _list_ops_dirs()
    run_root = _new_ops_root(before_dirs, after_dirs) or _latest_ops_root_after(start_ts)
    if run_root is None:
        return BenchResult(
            bench_name=spec.name,
            mode=mode,
            status="FAIL",
            failed_gates=["run_dir_missing"],
            warnings=[],
            ops_run_path="",
            loop_path="",
            before={},
            after={},
            gate_details={},
            notes=spec.notes,
            error="ops_cycle completed but no new ops_runs directory was detected",
        )

    loop_dir = _first_loop_dir(run_root)
    actions = _read_json(loop_dir / "ACTIONS.json")
    before = _read_json(loop_dir / "BEFORE_KPI.json")
    after = _read_json(loop_dir / "AFTER_KPI.json")

    mandatory = actions.get("mandatory_gates", {})
    failed_gates = [k for k, v in mandatory.items() if not bool(v)]
    status = "PASS" if (code == 0 and not failed_gates and actions.get("passed") is True) else "FAIL"
    warnings = _detect_warnings(before, after)
    return BenchResult(
        bench_name=spec.name,
        mode=mode,
        status=status,
        failed_gates=failed_gates,
        warnings=warnings,
        ops_run_path=str(run_root),
        loop_path=str(loop_dir),
        before=before,
        after=after,
        gate_details=actions.get("gate_details", {}),
        notes=spec.notes,
        error="",
    )


def _md_table_row(cells: list[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def _write_summary_md(report_dir: Path, results: list[BenchResult]) -> None:
    lines = [
        "# BENCH_SUMMARY",
        "",
        _md_table_row(["bench", "mode", "status", "failed_gates", "bad_domain_mix", "unknown_rate", "top50_effective_good_count", "ops_run_path"]),
        _md_table_row(["---"] * 8),
    ]
    for r in results:
        gd = r.gate_details
        bad = f"{gd.get('before_bad_domain_mix')} -> {gd.get('after_bad_domain_mix')}" if gd else "-"
        unknown = f"{r.before.get('rates', {}).get('unknown_rate')} -> {r.after.get('rates', {}).get('unknown_rate')}" if r.before and r.after else "-"
        eff = (
            f"{r.before.get('top50', {}).get('top50_effective_good_count')} -> {r.after.get('top50', {}).get('top50_effective_good_count')}"
            if r.before and r.after
            else "-"
        )
        lines.append(
            _md_table_row(
                [
                    r.bench_name,
                    r.mode,
                    r.status,
                    ", ".join(r.failed_gates) if r.failed_gates else "-",
                    bad,
                    unknown,
                    eff,
                    r.ops_run_path or "-",
                ]
            )
        )
    warn_lines = [f"- {r.bench_name}/{r.mode}: {w}" for r in results for w in r.warnings]
    if warn_lines:
        lines.extend(["", "## WARN", ""])
        lines.extend(warn_lines)
    (report_dir / "BENCH_SUMMARY.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_summary_json(report_dir: Path, results: list[BenchResult]) -> None:
    payload = {
        "generated_at_jst": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
        "results": [asdict(r) for r in results],
    }
    (report_dir / "BENCH_SUMMARY.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_failures_md(report_dir: Path, results: list[BenchResult]) -> None:
    failed = [r for r in results if r.status != "PASS"]
    if not failed:
        return
    lines = ["# FAILURES", ""]
    for r in failed:
        lines.extend(
            [
                f"## {r.bench_name} ({r.mode})",
                "",
                f"- status: `{r.status}`",
                f"- failed_gates: `{', '.join(r.failed_gates) if r.failed_gates else '-'}`",
                f"- ops_run_path: `{r.ops_run_path or '-'}`",
                f"- error: `{r.error or '-'}`",
            ]
        )
        if r.before and r.after:
            br, ar = r.before.get("rates", {}), r.after.get("rates", {})
            bt, at = r.before.get("top50", {}), r.after.get("top50", {})
            lines.extend(
                [
                    "",
                    "### Key KPI Delta",
                    "",
                    f"- bad_domain_mix: `{br.get('bad_domain_mix')}` -> `{ar.get('bad_domain_mix')}`",
                    f"- unknown_rate: `{br.get('unknown_rate')}` -> `{ar.get('unknown_rate')}`",
                    f"- solo_rate: `{br.get('solo_rate')}` -> `{ar.get('solo_rate')}`",
                    f"- corporate_rate: `{br.get('corporate_rate')}` -> `{ar.get('corporate_rate')}`",
                    f"- top50_effective_good_count: `{bt.get('top50_effective_good_count')}` -> `{at.get('top50_effective_good_count')}`",
                ]
            )
        if r.loop_path:
            report_path = Path(r.loop_path) / "AFTER_KPI_REPORT.md"
            if report_path.exists():
                md = report_path.read_text(encoding="utf-8")
                section1 = _extract_section(md, "## KPI Table (Phase 1)")
                section2 = _extract_section(md, "## BEFORE vs AFTER Delta")
                lines.extend(["", "### AFTER_KPI_REPORT Excerpt", ""])
                if section1:
                    lines.extend([section1, ""])
                if section2:
                    lines.extend([section2, ""])
        lines.append("")
    (report_dir / "FAILURES.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    modes = ["A", "B"] if args.mode == "both" else [args.mode]
    report_dir = Path(args.report_dir) if args.report_dir else OPS_RUNS_DIR / f"bench_{_ts()}"
    report_dir.mkdir(parents=True, exist_ok=True)

    results: list[BenchResult] = []
    for spec in BENCHES:
        for mode in modes:
            result = _run_one(spec, mode=mode, slice_override=args.slice)
            results.append(result)
            if args.fail_fast and result.status != "PASS":
                _write_summary_md(report_dir, results)
                _write_summary_json(report_dir, results)
                _write_failures_md(report_dir, results)
                print(f"[FAIL-FAST] stopped at {result.bench_name}/{mode}")
                print(f"[REPORT] {report_dir}")
                return 1

    _write_summary_md(report_dir, results)
    _write_summary_json(report_dir, results)
    _write_failures_md(report_dir, results)
    any_fail = any(r.status != "PASS" for r in results)
    print(f"[REPORT] {report_dir}")
    print("[RESULT] PASS" if not any_fail else "[RESULT] FAIL")
    return 1 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
