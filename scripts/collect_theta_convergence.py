#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OPS_RUNS = ROOT / "ops_runs"
REPORT_DIR = OPS_RUNS / "_reports"


@dataclass
class LoopMetrics:
    loop_id: str
    run_dir: str = ""
    passed: bool = False
    selected_candidate: str = ""
    theta_before: float | None = None
    theta_after: float | None = None
    theta_improved: bool | None = None
    bad_before: float | None = None
    bad_after: float | None = None
    top50_eff_before: int | None = None
    top50_eff_after: int | None = None
    unknown_after: float | None = None
    unknown_max: float | None = None
    stability_ran: bool | None = None
    stability_passed: bool | None = None


def _to_bool(value: str) -> bool | None:
    v = value.strip().lower()
    if v == "true":
        return True
    if v == "false":
        return False
    return None


def parse_loop_summary_text(text: str) -> list[LoopMetrics]:
    lines = text.splitlines()
    blocks: list[tuple[str, list[str]]] = []
    cur_id = ""
    cur_lines: list[str] = []
    for line in lines:
        m = re.match(r"^##\s+(loop_\d+)\s*$", line)
        if m:
            if cur_id:
                blocks.append((cur_id, cur_lines))
            cur_id = m.group(1)
            cur_lines = []
            continue
        if cur_id:
            cur_lines.append(line)
    if cur_id:
        blocks.append((cur_id, cur_lines))

    out: list[LoopMetrics] = []
    for loop_id, block_lines in blocks:
        mtr = LoopMetrics(loop_id=loop_id)
        for line in block_lines:
            m = re.match(r"^- run_dir: `(.+)`$", line)
            if m:
                mtr.run_dir = m.group(1)
                continue
            m = re.match(r"^- passed: `(.+)`$", line)
            if m:
                mtr.passed = bool(_to_bool(m.group(1)))
                continue
            m = re.match(r"^- selected_candidate: `(.+)`$", line)
            if m:
                mtr.selected_candidate = m.group(1)
                continue
            m = re.match(r"^- theta: `([^`]+)` -> `([^`]+)` improved=`([^`]+)`$", line)
            if m:
                mtr.theta_before = float(m.group(1))
                mtr.theta_after = float(m.group(2))
                mtr.theta_improved = _to_bool(m.group(3))
                continue
            m = re.match(r"^- bad_domain_mix: `([^`]+)` -> `([^`]+)`$", line)
            if m:
                mtr.bad_before = float(m.group(1))
                mtr.bad_after = float(m.group(2))
                continue
            m = re.match(r"^- top50_effective_good_count: `([^`]+)` -> `([^`]+)`$", line)
            if m:
                mtr.top50_eff_before = int(m.group(1))
                mtr.top50_eff_after = int(m.group(2))
                continue
            m = re.match(r"^- after_unknown_rate: `([^`]+)` \(max `([^`]+)`\)$", line)
            if m:
                mtr.unknown_after = float(m.group(1))
                mtr.unknown_max = float(m.group(2))
                continue
            m = re.match(r"^- stability: ran=`([^`]+)` passed=`([^`]+)` report=`(.+)`$", line)
            if m:
                mtr.stability_ran = _to_bool(m.group(1))
                mtr.stability_passed = _to_bool(m.group(2))
                continue
        out.append(mtr)
    return out


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _discover_latest_manifest() -> Path | None:
    manifests = sorted(REPORT_DIR.glob("theta_convergence_runs_*.tsv"))
    if not manifests:
        return None
    return manifests[-1]


def _load_run_dirs_from_manifest(manifest: Path) -> list[Path]:
    lines = manifest.read_text(encoding="utf-8").splitlines()
    out: list[Path] = []
    for line in lines[1:]:
        cols = line.split("\t")
        if len(cols) < 2:
            continue
        run_dir = cols[1].strip()
        if run_dir and run_dir != "(not_found)":
            out.append(Path(run_dir))
    return out


def _auto_discover_latest_k(k: int) -> list[Path]:
    candidates = [p for p in OPS_RUNS.iterdir() if p.is_dir() and p.name != "_reports" and (p / "LOOP_SUMMARY.md").exists()]
    candidates.sort(key=lambda p: p.name, reverse=True)
    return candidates[:k]


def _format_metric(before: Any, after: Any) -> str:
    if before is None or after is None:
        return "-"
    return f"{before} -> {after}"


def collect_rows(run_dirs: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run_dir in run_dirs:
        summary_path = run_dir / "LOOP_SUMMARY.md"
        if not summary_path.exists():
            continue
        loops = parse_loop_summary_text(summary_path.read_text(encoding="utf-8"))
        if not loops:
            continue
        improved_loops = [l for l in loops if l.theta_improved is True]
        passed_loops = [l for l in loops if l.passed]
        if improved_loops:
            chosen = max(
                improved_loops,
                key=lambda l: (l.theta_after if l.theta_after is not None else -1.0),
            )
        elif passed_loops:
            chosen = passed_loops[0]
        else:
            chosen = loops[-1]
        overall_passed = any(l.passed for l in loops)

        loop_actions = sorted(run_dir.glob("loop_*/ACTIONS.json"))
        input_csv = ""
        if loop_actions:
            input_csv = str(_read_json(loop_actions[0]).get("input_csv", ""))

        regressions: list[str] = []
        if not overall_passed:
            regressions.append("gate_failed")
        if chosen.stability_ran is True and chosen.stability_passed is False:
            regressions.append("stability_failed")
        if chosen.unknown_after is not None and chosen.unknown_max is not None and chosen.unknown_after > chosen.unknown_max:
            regressions.append("unknown_rate_exceeded")
        if chosen.top50_eff_before is not None and chosen.top50_eff_after is not None and chosen.top50_eff_after < chosen.top50_eff_before:
            regressions.append("top50_effective_degraded")
        if chosen.theta_improved is False:
            regressions.append("theta_not_improved")

        theta_delta = None
        if chosen.theta_before is not None and chosen.theta_after is not None:
            theta_delta = chosen.theta_after - chosen.theta_before

        rows.append(
            {
                "run_dir": str(run_dir),
                "csv": input_csv,
                "selected_candidate": chosen.selected_candidate,
                "passed": overall_passed,
                "theta_before": chosen.theta_before,
                "theta_after": chosen.theta_after,
                "theta_delta": theta_delta,
                "theta_improved": chosen.theta_improved,
                "bad_before": chosen.bad_before,
                "bad_after": chosen.bad_after,
                "top50_eff_before": chosen.top50_eff_before,
                "top50_eff_after": chosen.top50_eff_after,
                "unknown_after": chosen.unknown_after,
                "unknown_max": chosen.unknown_max,
                "stability_ran": chosen.stability_ran,
                "stability_passed": chosen.stability_passed,
                "regressions": regressions,
            }
        )
    return rows


def write_report(rows: list[dict[str, Any]], out_path: Path) -> None:
    total = len(rows)
    pass_count = sum(1 for r in rows if r["passed"])
    regress_count = sum(1 for r in rows if r["regressions"])

    lines = [
        "# THETA Convergence Report",
        "",
        f"- generated_at: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
        f"- runs: `{total}`",
        f"- pass_rate: `{pass_count}/{total}`" if total else "- pass_rate: `0/0`",
        f"- regression_rows: `{regress_count}`",
        "",
        "## Run Table",
        "",
        "| csv | run_dir | passed | theta (before->after) | bad_domain_mix | top50_effective_good | unknown_rate | stability | selected_candidate | regressions |",
        "|---|---|---:|---|---|---|---|---|---|---|",
    ]

    for r in rows:
        theta_text = "-"
        if r["theta_before"] is not None and r["theta_after"] is not None:
            theta_text = f"{r['theta_before']:.6f}->{r['theta_after']:.6f} ({r['theta_delta']:+.6f})"
        bad_text = _format_metric(r["bad_before"], r["bad_after"])
        top50_text = _format_metric(r["top50_eff_before"], r["top50_eff_after"])
        unknown_text = "-"
        if r["unknown_after"] is not None and r["unknown_max"] is not None:
            unknown_text = f"{r['unknown_after']:.6f}/{r['unknown_max']:.6f}"
        stability_text = f"{r['stability_ran']}/{r['stability_passed']}"
        regress_text = ",".join(r["regressions"]) if r["regressions"] else "-"
        csv_name = Path(r["csv"]).name if r["csv"] else "-"
        run_name = Path(r["run_dir"]).name
        lines.append(
            f"| {csv_name} | `{run_name}` | {str(r['passed']).lower()} | {theta_text} | {bad_text} | {top50_text} | {unknown_text} | {stability_text} | {r['selected_candidate'] or '-'} | {regress_text} |"
        )

    flagged = [r for r in rows if r["regressions"]]
    lines.extend(["", "## Regression Flags", ""])
    if not flagged:
        lines.append("- none")
    else:
        for r in flagged:
            lines.append(f"- `{r['run_dir']}`: {', '.join(r['regressions'])}")

    def _worst_key(r: dict[str, Any]) -> tuple[int, float]:
        score = len(r["regressions"])
        delta = r["theta_delta"] if r["theta_delta"] is not None else -999.0
        return (-score, delta)

    worst = sorted(rows, key=_worst_key)[:3]
    lines.extend(["", "## Top 3 Worst Runs", ""])
    for idx, r in enumerate(worst, start=1):
        lines.append(
            f"{idx}. `{r['run_dir']}` (regressions={len(r['regressions'])}, theta_delta={r['theta_delta'] if r['theta_delta'] is not None else 'n/a'})"
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect theta convergence results into a single markdown report")
    p.add_argument("--runs", nargs="*", default=[], help="Explicit ops_runs/<run_id> directories")
    p.add_argument("--manifest", default="", help="TSV manifest generated by bench_theta_convergence.sh")
    p.add_argument("--latest-k", type=int, default=12, help="Auto-discover newest K runs when manifest/runs not provided")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    run_dirs: list[Path] = []
    if args.runs:
        run_dirs = [Path(x) for x in args.runs]
    elif args.manifest:
        run_dirs = _load_run_dirs_from_manifest(Path(args.manifest))
    else:
        manifest = _discover_latest_manifest()
        if manifest is not None:
            run_dirs = _load_run_dirs_from_manifest(manifest)
        if not run_dirs:
            run_dirs = _auto_discover_latest_k(args.latest_k)

    run_dirs = [p for p in run_dirs if p.exists() and (p / "LOOP_SUMMARY.md").exists()]
    rows = collect_rows(run_dirs)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = REPORT_DIR / f"THETA_CONVERGENCE_{ts}.md"
    write_report(rows, out_path)
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
