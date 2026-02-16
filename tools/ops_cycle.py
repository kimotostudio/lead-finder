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
import difflib
import hashlib
import json
import re
import shutil
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from tools import kpi_generate
from tools.patch_engine import PatchPlan, PatchResult, apply_patch, propose_patch, revert_patch
from tools.theta import (
    GATE_POLICY,
    GLOBAL_MEDIA_NOISE_DOMAINS,
    NOISE_DOMAIN_SUFFIXES,
    get_theta_snapshot,
    normalize_domain,
)


JST = ZoneInfo("Asia/Tokyo")
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "web_app" / "output" / "merge_fukuoka_all_queries.csv"
OPS_RUNS_DIR = ROOT / "ops_runs"
TOP50_CAP = 50.0
# Progress objective Theta (maximize):
# - lower-is-better metrics are flipped with (1 - rate)
# - top50_effective_good_count is normalized by top50 cap
PROGRESS_THETA_WEIGHTS = {
    "bad_domain_mix": 0.40,
    "unknown_rate": 0.20,
    "top50_effective_good_count": 0.25,
    "solo_rate": 0.10,
    "corporate_rate": 0.05,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one OPS precision-first cycle.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input merged CSV path")
    parser.add_argument("--slice", type=int, default=200, help="Deterministic safe slice size (first N rows)")
    parser.add_argument("--loop", type=int, default=1, help="Number of optimization loops")
    parser.add_argument("--mode", choices=("A", "B"), default="A", help="Optimization mode")
    parser.add_argument("--max-candidates", "--candidates", dest="max_candidates", type=int, default=3, help="Max candidate attempts per loop")
    parser.add_argument("--unknown-rate-max", type=float, default=0.20, help="Mandatory gate threshold for unknown_rate")
    parser.add_argument("--stability-slice", type=int, default=200, help="Deterministic slice size for stability verification")
    parser.add_argument("--no-stability", action="store_true", help="Disable stability verification run")
    parser.add_argument("--no-progress-k", type=int, default=3, help="Stop when Theta does not improve for K loops")
    parser.add_argument("--val-inputs", action="append", default=[], help="Validation input CSV path (repeatable)")
    parser.add_argument("--val-manifest", default="", help="Optional manifest file with one validation CSV path per line")
    return parser.parse_args()


def _ts() -> str:
    return datetime.now(JST).strftime("%Y%m%d_%H%M%S_%f")


def _log(line: str, log_path: Path) -> None:
    timestamp = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    text = f"[{timestamp}] {line}"
    print(text)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(text + "\n")


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def compute_progress_theta(kpi: dict[str, Any]) -> dict[str, Any]:
    rates = kpi.get("rates", {})
    top50 = kpi.get("top50", {})
    bad_mix = float(rates.get("bad_domain_mix", 1.0))
    unknown_rate = float(rates.get("unknown_rate", 1.0))
    solo_rate = float(rates.get("solo_rate", 0.0))
    corporate_raw = rates.get("corporate_rate")
    corporate_rate = float(corporate_raw) if corporate_raw is not None else 0.5
    top50_effective = int(top50.get("top50_effective_good_count", top50.get("top50_good_count", 0)))

    components = {
        "bad_domain_mix_term": PROGRESS_THETA_WEIGHTS["bad_domain_mix"] * (1.0 - _clamp01(bad_mix)),
        "unknown_rate_term": PROGRESS_THETA_WEIGHTS["unknown_rate"] * (1.0 - _clamp01(unknown_rate)),
        "top50_effective_good_count_term": PROGRESS_THETA_WEIGHTS["top50_effective_good_count"] * _clamp01(top50_effective / TOP50_CAP),
        "solo_rate_term": PROGRESS_THETA_WEIGHTS["solo_rate"] * _clamp01(solo_rate),
        "corporate_rate_term": PROGRESS_THETA_WEIGHTS["corporate_rate"] * (1.0 - _clamp01(corporate_rate)),
    }
    theta = round(sum(components.values()), 12)
    return {
        "theta": theta,
        "weights": dict(PROGRESS_THETA_WEIGHTS),
        "inputs": {
            "bad_domain_mix": bad_mix,
            "unknown_rate": unknown_rate,
            "top50_effective_good_count": top50_effective,
            "solo_rate": solo_rate,
            "corporate_rate": corporate_rate,
        },
        "components": components,
    }


def theta_improved(theta_before: float, theta_after: float, epsilon: float = 1e-12) -> bool:
    return float(theta_after) > (float(theta_before) + epsilon)


def should_accept_patch(
    *,
    mandatory_gates: dict[str, bool],
    theta_before: float,
    theta_after: float,
) -> tuple[bool, bool]:
    improved = theta_improved(theta_before, theta_after)
    gates_passed = all(bool(v) for v in mandatory_gates.values())
    return gates_passed and improved, improved


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


def _row_to_lead_for_filter(row: dict[str, str]) -> dict[str, str]:
    return {
        "url": row.get("URL") or row.get("url") or row.get("final_url") or row.get("最終URL") or "",
        "shop_name": row.get("店舗名") or row.get("shop_name") or row.get("title") or "",
        "title": row.get("title") or row.get("店舗名") or row.get("shop_name") or "",
        "visible_text": row.get("visible_text") or row.get("reasons") or row.get("営業ラベル理由") or "",
        "reasons": row.get("reasons") or row.get("営業ラベル理由") or "",
    }


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url if "://" in url else f"https://{url}")
    return normalize_domain(parsed.netloc or "")


def _load_ops_auto_domains(filters_path: Path) -> set[str]:
    if not filters_path.exists():
        return set()
    text = filters_path.read_text(encoding="utf-8", errors="replace")
    m = re.search(
        r"# OPS_AUTO_BLOCKLIST_START\s*OPS_AUTO_EXCLUDED_DOMAINS\s*=\s*\{(.*?)\}\s*# OPS_AUTO_BLOCKLIST_END",
        text,
        flags=re.DOTALL,
    )
    if not m:
        return set()
    return {d.strip().lower() for d in re.findall(r"'([^']+)'", m.group(1)) if "." in d}


def _looks_local_business_pattern(lead: dict[str, str]) -> bool:
    blob = " ".join(
        str(v).lower()
        for v in (
            lead.get("url", ""),
            lead.get("title", ""),
            lead.get("shop_name", ""),
            lead.get("visible_text", ""),
            lead.get("reasons", ""),
        )
    )
    local_markers = ("サロン", "整体", "美容", "カウンセリング", "セラピー", "個人", "予約", "福岡", "市", "区", "salon", "therapy", "counseling", "private")
    return any(m.lower() in blob for m in local_markers)


def _is_filtered_by_current_ops_rules(lead: dict[str, str], ops_auto_domains: set[str]) -> bool:
    domain = _extract_domain(lead.get("url", ""))
    if not domain:
        return False

    if any(domain.endswith(suffix) for suffix in NOISE_DOMAIN_SUFFIXES):
        return True

    is_global_media = any(domain == d or domain.endswith("." + d) for d in GLOBAL_MEDIA_NOISE_DOMAINS)
    if is_global_media and not _looks_local_business_pattern(lead):
        return True

    for d in ops_auto_domains:
        if domain == d or domain.endswith("." + d):
            if is_global_media:
                return False
            return True
    return False


def build_after_slice_with_current_filter(input_csv: Path, out_csv: Path) -> tuple[int, int]:
    ops_auto_domains = _load_ops_auto_domains(ROOT / "src" / "filters.py")
    with input_csv.open("r", encoding="utf-8-sig", newline="") as rf:
        reader = csv.DictReader(rf)
        fieldnames = reader.fieldnames or []
        kept_rows: list[dict[str, str]] = []
        total = 0
        for row in reader:
            total += 1
            lead = _row_to_lead_for_filter(row)
            if not _is_filtered_by_current_ops_rules(lead, ops_auto_domains):
                kept_rows.append(row)

    with out_csv.open("w", encoding="utf-8-sig", newline="") as wf:
        writer = csv.DictWriter(wf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept_rows)
    return total, len(kept_rows)


def gate_result(
    before: dict[str, Any],
    after: dict[str, Any],
    unknown_rate_max: float,
    gate_policy: dict[str, bool] | None = None,
) -> tuple[bool, dict[str, bool], dict[str, bool], dict[str, Any]]:
    policy = gate_policy or GATE_POLICY
    before_bad = float(before.get("rates", {}).get("bad_domain_mix", 1.0))
    after_bad = float(after.get("rates", {}).get("bad_domain_mix", 1.0))
    before_city_missing = float(before.get("rates", {}).get("city_missing_rate", 1.0))
    after_city_missing = float(after.get("rates", {}).get("city_missing_rate", 1.0))
    before_solo = float(before.get("rates", {}).get("solo_rate", 0.0))
    after_solo = float(after.get("rates", {}).get("solo_rate", 0.0))
    after_unknown = float(after.get("rates", {}).get("unknown_rate", 1.0))
    before_top50 = int(before.get("top50", {}).get("top50_good_count", 0))
    after_top50 = int(after.get("top50", {}).get("top50_good_count", 0))
    before_top50_bad_domain = int(before.get("top50", {}).get("top50_bad_domain_count", 0))
    after_top50_bad_domain = int(after.get("top50", {}).get("top50_bad_domain_count", 0))
    before_top50_effective = int(before.get("top50", {}).get("top50_effective_good_count", before_top50))
    after_top50_effective = int(after.get("top50", {}).get("top50_effective_good_count", after_top50))
    good_drop = max(0, before_top50 - after_top50)
    noise_drop = max(0, before_top50_bad_domain - after_top50_bad_domain)
    top50_good_drop_explained = good_drop <= noise_drop

    mandatory_gates: dict[str, bool] = {}
    if policy.get("require_bad_domain_mix_non_increasing", True):
        mandatory_gates["bad_domain_mix_non_increasing"] = after_bad <= before_bad
    if policy.get("require_city_missing_rate_non_worsening", True):
        mandatory_gates["city_missing_rate_non_increasing"] = after_city_missing <= before_city_missing
    if policy.get("require_solo_rate_non_worsening", True):
        mandatory_gates["solo_rate_non_decreasing"] = after_solo >= before_solo
    if policy.get("allow_top50_good_drop_if_explained_by_noise_removed", True):
        mandatory_gates["top50_good_drop_explained_by_noise_removed"] = top50_good_drop_explained
    else:
        mandatory_gates["top50_good_count_non_decreasing"] = after_top50 >= before_top50

    advisory_gates = {
        "top50_good_count_non_decreasing": after_top50 >= before_top50,
        "top50_effective_good_count_non_decreasing": after_top50_effective >= before_top50_effective,
        "unknown_rate_threshold_ok": after_unknown <= unknown_rate_max,
    }
    passed = all(mandatory_gates.values())
    gate_reasons = [f"failed:{k}" for k, v in mandatory_gates.items() if not v]
    if not gate_reasons:
        gate_reasons = ["all_mandatory_gates_passed"]
    details = {
        "before_bad_domain_mix": before_bad,
        "after_bad_domain_mix": after_bad,
        "before_city_missing_rate": before_city_missing,
        "after_city_missing_rate": after_city_missing,
        "before_solo_rate": before_solo,
        "after_solo_rate": after_solo,
        "after_unknown_rate": after_unknown,
        "unknown_rate_max": unknown_rate_max,
        "before_top50_good_count": before_top50,
        "after_top50_good_count": after_top50,
        "before_top50_noise_count": before_top50_bad_domain,
        "after_top50_noise_count": after_top50_bad_domain,
        "before_top50_bad_domain_count": before_top50_bad_domain,
        "after_top50_bad_domain_count": after_top50_bad_domain,
        "top50_good_drop": good_drop,
        "top50_bad_domain_drop": noise_drop,
        "top50_good_drop_explained": top50_good_drop_explained,
        "before_top50_effective_good_count": before_top50_effective,
        "after_top50_effective_good_count": after_top50_effective,
        "gate_reasons": gate_reasons,
    }
    return passed, mandatory_gates, advisory_gates, details


def write_summary(
    summary_path: Path,
    before: dict[str, Any],
    after: dict[str, Any],
    patch_target_file: Path,
    candidate_domains: list[str],
    patch_applied: bool,
    patch_message: str,
    gates_passed: bool,
    mandatory_gates: dict[str, bool],
    advisory_gates: dict[str, bool],
    gate_details: dict[str, Any],
    theta_hash: str,
    reverted: bool,
) -> None:
    lines = [
        "# OPS Cycle Summary",
        "",
        f"- target_file: `{patch_target_file}`",
        f"- candidate_domains: `{', '.join(candidate_domains) if candidate_domains else '(none)'}`",
        f"- patch_applied: `{patch_applied}`",
        f"- patch_message: `{patch_message}`",
        f"- theta_hash: `{theta_hash}`",
        f"- theta_before: `{gate_details.get('theta_before')}`",
        f"- theta_after: `{gate_details.get('theta_after')}`",
        f"- theta_improved: `{gate_details.get('theta_improved')}`",
        f"- gates_passed: `{gates_passed}`",
        f"- reverted: `{reverted}`",
        "",
        "## KPI Before -> After",
        "",
        f"- bad_domain_mix: `{gate_details['before_bad_domain_mix']}` -> `{gate_details['after_bad_domain_mix']}`",
        f"- city_missing_rate: `{gate_details['before_city_missing_rate']}` -> `{gate_details['after_city_missing_rate']}`",
        f"- solo_rate: `{gate_details['before_solo_rate']}` -> `{gate_details['after_solo_rate']}`",
        f"- unknown_rate: `<= {gate_details['unknown_rate_max']}` (after `{gate_details['after_unknown_rate']}`)",
        f"- top50_good_count: `{gate_details['before_top50_good_count']}` -> `{gate_details['after_top50_good_count']}`",
        f"- top50_noise_count: `{gate_details['before_top50_noise_count']}` -> `{gate_details['after_top50_noise_count']}`",
        (
            "- allowed_good_drop_logic: "
            f"`good_drop={gate_details['top50_good_drop']}` <= "
            f"`noise_drop={gate_details['top50_bad_domain_drop']}` "
            f"(pass=`{gate_details['top50_good_drop_explained']}`)"
        ),
        f"- top50_effective_good_count: `{gate_details['before_top50_effective_good_count']}` -> `{gate_details['after_top50_effective_good_count']}`",
        f"- gate_reasons: `{', '.join(gate_details.get('gate_reasons', []))}`",
        "",
        "## Mandatory Gates",
        "",
    ]
    for key, value in mandatory_gates.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Advisory Gates", ""])
    for key, value in advisory_gates.items():
        lines.append(f"- {key}: `{value}`")

    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _append_after_kpi_delta_report(path: Path, before: dict[str, Any], after: dict[str, Any]) -> None:
    before_rates = before.get("rates", {})
    after_rates = after.get("rates", {})
    before_top50 = before.get("top50", {})
    after_top50 = after.get("top50", {})

    def _rate_delta(key: str) -> str:
        b = float(before_rates.get(key, 0.0))
        a = float(after_rates.get(key, 0.0))
        return f"- {key}: `{b:.6f}` -> `{a:.6f}` (delta `{(a - b):+.6f}`)"

    def _count_delta(key: str) -> str:
        b = int(before_top50.get(key, 0))
        a = int(after_top50.get(key, 0))
        return f"- {key}: `{b}` -> `{a}` (delta `{a - b:+d}`)"

    lines = [
        "",
        "## BEFORE vs AFTER Delta",
        "",
        _rate_delta("bad_domain_mix"),
        _rate_delta("solo_rate"),
        _rate_delta("corporate_rate"),
        _rate_delta("unknown_rate"),
        _rate_delta("city_missing_rate"),
        _count_delta("top50_good_count"),
        _count_delta("top50_effective_good_count"),
        _count_delta("top50_bad_domain_count"),
        _count_delta("top50_city_missing_count"),
    ]
    with path.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _should_run_stability(small_slice_passed: bool, stability_enabled: bool) -> bool:
    return small_slice_passed and stability_enabled


def _merge_stability_gate(mandatory_gates: dict[str, bool], stability_enabled: bool, stability_passed: bool | None) -> dict[str, bool]:
    merged = dict(mandatory_gates)
    if stability_enabled:
        merged["stability_passed"] = bool(stability_passed)
    return merged


def _merge_val_gate(mandatory_gates: dict[str, bool], val_enabled: bool, val_passed: bool | None) -> dict[str, bool]:
    merged = dict(mandatory_gates)
    if val_enabled:
        merged["val_passed"] = bool(val_passed)
    return merged


def _failing_gate_key(mandatory_gates: dict[str, bool]) -> str:
    for key, value in mandatory_gates.items():
        if not bool(value):
            return key
    return ""


def _should_early_stop(passed_small: bool, stability_passed: bool) -> bool:
    return passed_small and stability_passed


def _next_no_progress_streak(theta_has_improved: bool, prev_streak: int) -> int:
    if theta_has_improved:
        return 0
    return prev_streak + 1


def _write_next_action(root_dir: Path, failing_gate_key: str, mode: str) -> Path:
    if failing_gate_key == "unknown_rate_threshold_ok":
        proposal = "- propose: add one strong unknown->corporate marker in `tools/kpi_generate.py` (single keyword, no filtering side-effect)"
    elif failing_gate_key == "bad_domain_mix_non_increasing":
        proposal = "- propose: add 1-2 obvious noise domains from latest bad_domain candidates to `OPS_AUTO_EXCLUDED_DOMAINS` in `src/filters.py`"
    elif failing_gate_key == "solo_rate_non_decreasing":
        proposal = "- propose: add one conservative solo signal in `src/scoring_rules.py` for reservation/profile context"
    elif failing_gate_key == "city_missing_rate_non_increasing":
        proposal = "- propose: relax city missing penalty handling for known local-business pages in `tools/kpi_generate.py`"
    elif failing_gate_key == "top50_good_drop_explained_by_noise_removed":
        proposal = "- propose: tighten only hard-noise detection to avoid dropping borderline good leads in top50"
    else:
        proposal = "- propose: apply one minimal Mode B candidate patch and re-evaluate"
    lines = [
        "# NEXT_ACTION",
        "",
        f"- mode: `{mode}`",
        f"- failing_gate_key: `{failing_gate_key or 'unknown'}`",
        "",
        "## Proposed Minimal Patch",
        "",
        proposal,
        "- note: this is a proposal only; patch is not auto-applied.",
    ]
    out = root_dir / "NEXT_ACTION.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def _resolve_val_inputs(cli_values: list[str], manifest_path: str) -> list[Path]:
    paths: list[str] = list(cli_values or [])
    if manifest_path:
        mpath = Path(manifest_path)
        if mpath.exists():
            for line in mpath.read_text(encoding="utf-8").splitlines():
                value = line.strip()
                if not value or value.startswith("#"):
                    continue
                paths.append(value)
    seen: set[str] = set()
    resolved: list[Path] = []
    for raw in paths:
        s = str(raw).strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        resolved.append(Path(s))
    return resolved


def _inject_theta_snapshot(kpi_path: Path, kpi_data: dict[str, Any], theta_snapshot: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(kpi_data)
    enriched["theta_snapshot"] = theta_snapshot
    kpi_path.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")
    return enriched


def _run_stability_check(
    *,
    run_dir: Path,
    input_path: Path,
    stability_slice_n: int,
    theta_snapshot: dict[str, Any],
    unknown_rate_max: float,
    log_path: Path,
) -> dict[str, Any]:
    stability_slice_csv = run_dir / "stability_slice_input.csv"
    stability_after_slice_csv = run_dir / "stability_after_slice_filtered.csv"
    stability_before_json = run_dir / "STABILITY_BEFORE_KPI.json"
    stability_before_md = run_dir / "STABILITY_BEFORE_KPI_REPORT.md"
    stability_after_json = run_dir / "STABILITY_AFTER_KPI.json"
    stability_report_md = run_dir / "STABILITY_KPI_REPORT.md"

    stability_rows = copy_slice(input_path, stability_slice_csv, stability_slice_n)
    _log(f"stability slice rows written: {stability_rows} -> {stability_slice_csv}", log_path)
    stability_before = kpi_generate.run(stability_slice_csv, stability_before_json, stability_before_md, slice_value=None)
    stability_before = _inject_theta_snapshot(stability_before_json, stability_before, theta_snapshot)
    before_rows, kept_rows = build_after_slice_with_current_filter(stability_slice_csv, stability_after_slice_csv)
    _log(f"stability after filter applied: kept={kept_rows}/{before_rows} -> {stability_after_slice_csv}", log_path)
    stability_after = kpi_generate.run(stability_after_slice_csv, stability_after_json, stability_report_md, slice_value=None)
    stability_after = _inject_theta_snapshot(stability_after_json, stability_after, theta_snapshot)
    _append_after_kpi_delta_report(stability_report_md, stability_before, stability_after)
    stability_passed, stability_mandatory, stability_advisory, stability_details = gate_result(
        stability_before,
        stability_after,
        unknown_rate_max,
        gate_policy=theta_snapshot.get("gate_policy", {}),
    )
    return {
        "ran": True,
        "passed": stability_passed,
        "slice_n": stability_slice_n,
        "slice_csv": str(stability_slice_csv),
        "after_slice_csv": str(stability_after_slice_csv),
        "before_kpi": str(stability_before_json),
        "after_kpi": str(stability_after_json),
        "report": str(stability_report_md),
        "mandatory_gates": stability_mandatory,
        "advisory_gates": stability_advisory,
        "gate_details": stability_details,
    }


def _write_val_artifacts(run_dir: Path, val_actions: dict[str, Any]) -> None:
    (run_dir / "VAL_ACTIONS.json").write_text(json.dumps(val_actions, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# VAL_SUMMARY",
        "",
        f"- enabled: `{val_actions.get('enabled')}`",
        f"- all_passed: `{val_actions.get('all_passed')}`",
        f"- slice_n: `{val_actions.get('slice_n')}`",
        "",
    ]
    for r in val_actions.get("results", []):
        lines.extend(
            [
                f"## {r.get('name')}",
                f"- input: `{r.get('input')}`",
                f"- passed: `{r.get('passed')}`",
                f"- gate_reason: `{r.get('gate_reason')}`",
                f"- theta_hash: `{r.get('theta_hash')}`",
                (
                    f"- bad_domain_mix: `{r.get('before_bad_domain_mix')}` -> "
                    f"`{r.get('after_bad_domain_mix')}`"
                ),
                (
                    f"- top50_effective_good_count: `{r.get('before_top50_effective_good_count')}` -> "
                    f"`{r.get('after_top50_effective_good_count')}`"
                ),
                f"- unknown_rate: `{r.get('after_unknown_rate')}` (max `{r.get('unknown_rate_max')}`)",
                "",
            ]
        )
    (run_dir / "VAL_SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")


def _run_validation_gate(
    *,
    run_dir: Path,
    val_inputs: list[Path],
    input_slice_n: int,
    theta_snapshot: dict[str, Any],
    theta_hash: str,
    unknown_rate_max: float,
    log_path: Path,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for idx, val_input in enumerate(val_inputs, start=1):
        name = f"val_{idx:02d}"
        item: dict[str, Any] = {
            "name": name,
            "input": str(val_input),
            "theta_hash": theta_hash,
        }
        if not val_input.exists():
            item["passed"] = False
            item["gate_reason"] = "input_missing"
            results.append(item)
            continue
        slice_csv = run_dir / f"VAL_{idx:02d}_slice_input.csv"
        after_slice_csv = run_dir / f"VAL_{idx:02d}_after_slice_filtered.csv"
        before_json = run_dir / f"VAL_{idx:02d}_BEFORE_KPI.json"
        before_md = run_dir / f"VAL_{idx:02d}_BEFORE_KPI_REPORT.md"
        after_json = run_dir / f"VAL_{idx:02d}_AFTER_KPI.json"
        after_md = run_dir / f"VAL_{idx:02d}_KPI_REPORT.md"
        val_rows = copy_slice(val_input, slice_csv, input_slice_n)
        _log(f"val[{idx}] slice rows written: {val_rows} -> {slice_csv}", log_path)
        before = kpi_generate.run(slice_csv, before_json, before_md, slice_value=None)
        before = _inject_theta_snapshot(before_json, before, theta_snapshot)
        base_rows, kept_rows = build_after_slice_with_current_filter(slice_csv, after_slice_csv)
        _log(f"val[{idx}] after filter applied: kept={kept_rows}/{base_rows} -> {after_slice_csv}", log_path)
        after = kpi_generate.run(after_slice_csv, after_json, after_md, slice_value=None)
        after = _inject_theta_snapshot(after_json, after, theta_snapshot)
        _append_after_kpi_delta_report(after_md, before, after)
        before_bad = float(before.get("rates", {}).get("bad_domain_mix", 1.0))
        after_bad = float(after.get("rates", {}).get("bad_domain_mix", 1.0))
        before_eff = int(before.get("top50", {}).get("top50_effective_good_count", 0))
        after_eff = int(after.get("top50", {}).get("top50_effective_good_count", 0))
        after_unknown = float(after.get("rates", {}).get("unknown_rate", 1.0))
        val_gates = {
            "bad_domain_mix_non_increasing": after_bad <= before_bad,
            "top50_effective_good_count_non_decreasing": after_eff >= before_eff,
            "unknown_rate_threshold_ok": after_unknown <= unknown_rate_max,
        }
        failed_keys = [k for k, v in val_gates.items() if not v]
        item.update(
            {
                "passed": not failed_keys,
                "gate_reason": "all_val_gates_passed" if not failed_keys else f"failed:{failed_keys[0]}",
                "gate_reasons": failed_keys,
                "before_bad_domain_mix": before_bad,
                "after_bad_domain_mix": after_bad,
                "before_top50_effective_good_count": before_eff,
                "after_top50_effective_good_count": after_eff,
                "after_unknown_rate": after_unknown,
                "unknown_rate_max": unknown_rate_max,
                "before_kpi": str(before_json),
                "after_kpi": str(after_json),
                "report": str(after_md),
            }
        )
        results.append(item)
    all_passed = all(bool(r.get("passed")) for r in results) if results else True
    payload = {
        "enabled": True,
        "slice_n": input_slice_n,
        "all_passed": all_passed,
        "theta_hash": theta_hash,
        "results": results,
    }
    _write_val_artifacts(run_dir, payload)
    return payload


@dataclass
class Candidate:
    kind: str
    name: str
    payload: dict[str, Any]


def _load_corporate_keywords_from_filters(filters_path: Path) -> list[str]:
    if not filters_path.exists():
        return []
    text = filters_path.read_text(encoding="utf-8", errors="replace")
    m = re.search(r"CORPORATE_KEYWORDS\s*=\s*\[(.*?)\]", text, flags=re.DOTALL)
    if not m:
        return []
    return re.findall(r"'([^']+)'", m.group(1))


def _load_unknown_markers_from_kpi(kpi_path: Path) -> list[str]:
    if not kpi_path.exists():
        return []
    text = kpi_path.read_text(encoding="utf-8", errors="replace")
    m = re.search(r"UNKNOWN_TO_CORPORATE_STRONG_MARKERS\s*=\s*\((.*?)\)", text, flags=re.DOTALL)
    if not m:
        return []
    return re.findall(r'"([^"]+)"', m.group(1))


def _apply_corporate_keyword_candidate(term: str, run_dir: Path) -> PatchResult:
    target_file = ROOT / "src" / "filters.py"
    original = target_file.read_text(encoding="utf-8", errors="replace")
    if term in original:
        return PatchResult(
            applied=False,
            target_file=target_file,
            backup_file=None,
            diff_text="",
            domains_added=[],
            message=f"patch_empty:corporate_keyword_already_exists:{term}",
        )

    m = re.search(r"(CORPORATE_KEYWORDS\s*=\s*\[)(.*?)(\n\])", original, flags=re.DOTALL)
    if not m:
        return PatchResult(
            applied=False,
            target_file=target_file,
            backup_file=None,
            diff_text="",
            domains_added=[],
            message="patch_empty:corporate_keywords_block_missing",
        )

    body = m.group(2)
    insertion = f"\n    '{term}',"
    updated = original[:m.start(2)] + body + insertion + original[m.end(2):]
    if updated == original:
        return PatchResult(
            applied=False,
            target_file=target_file,
            backup_file=None,
            diff_text="",
            domains_added=[],
            message="patch_empty:no_content_change",
        )

    backup = run_dir / f"{target_file.name}.{term}.bak"
    backup.write_text(original, encoding="utf-8")
    target_file.write_text(updated, encoding="utf-8")
    diff_text = "".join(
        difflib.unified_diff(
            original.splitlines(keepends=True),
            updated.splitlines(keepends=True),
            fromfile=str(target_file),
            tofile=str(target_file),
        )
    )
    return PatchResult(
        applied=True,
        target_file=target_file,
        backup_file=backup,
        diff_text=diff_text,
        domains_added=[],
        message=f"applied:corporate_keyword:{term}",
    )


def _apply_unknown_marker_candidate(term: str, run_dir: Path) -> PatchResult:
    target_file = ROOT / "tools" / "kpi_generate.py"
    original = target_file.read_text(encoding="utf-8", errors="replace")
    if f'"{term}"' in original:
        return PatchResult(
            applied=False,
            target_file=target_file,
            backup_file=None,
            diff_text="",
            domains_added=[],
            message=f"patch_empty:unknown_marker_already_exists:{term}",
        )

    m = re.search(r"(UNKNOWN_TO_CORPORATE_STRONG_MARKERS\s*=\s*\()(.*?)(\n\))", original, flags=re.DOTALL)
    if not m:
        return PatchResult(
            applied=False,
            target_file=target_file,
            backup_file=None,
            diff_text="",
            domains_added=[],
            message="patch_empty:unknown_marker_block_missing",
        )

    body = m.group(2)
    insertion = f'\n    "{term}",'
    updated = original[:m.start(2)] + body + insertion + original[m.end(2):]
    if updated == original:
        return PatchResult(
            applied=False,
            target_file=target_file,
            backup_file=None,
            diff_text="",
            domains_added=[],
            message="patch_empty:no_content_change",
        )

    backup = run_dir / f"{target_file.name}.{term}.bak"
    backup.write_text(original, encoding="utf-8")
    target_file.write_text(updated, encoding="utf-8")
    diff_text = "".join(
        difflib.unified_diff(
            original.splitlines(keepends=True),
            updated.splitlines(keepends=True),
            fromfile=str(target_file),
            tofile=str(target_file),
        )
    )
    return PatchResult(
        applied=True,
        target_file=target_file,
        backup_file=backup,
        diff_text=diff_text,
        domains_added=[],
        message=f"applied:unknown_marker:{term}",
    )


def _build_mode_b_candidates(before: dict[str, Any], max_candidates: int) -> list[Candidate]:
    filters_path = ROOT / "src" / "filters.py"
    kpi_path = ROOT / "tools" / "kpi_generate.py"
    existing = set(_load_corporate_keywords_from_filters(filters_path))
    existing_unknown_markers = set(_load_unknown_markers_from_kpi(kpi_path))
    priority_b_terms = [
        "公益財団法人",
        "一般財団法人",
        "公益社団法人",
        "一般社団法人",
        "社会福祉法人",
        "医療法人",
        "学校法人",
        "特定非営利活動法人",
        "NPO法人",
        "協同組合",
        "連合会",
        "事業団",
        "公社",
        "公団",
        "行政",
        "自治体",
        "商工会",
    ]
    priority_a_terms = [
        "社会福祉協議会",
        "商工会議所",
        "協会",
        "振興会",
        "組合",
    ]
    candidates: list[Candidate] = []
    for term in priority_b_terms:
        if term not in existing_unknown_markers:
            candidates.append(Candidate(kind="unknown_marker", name=f"B:{term}", payload={"term": term}))

    for term in priority_a_terms:
        if term not in existing:
            candidates.append(Candidate(kind="corporate_keyword", name=f"A:{term}", payload={"term": term}))

    patch_plan = propose_patch(ROOT, before, max_domains=5)
    candidates.append(
        Candidate(
            kind="domain_block",
            name="C:domain_block_from_kpi",
            payload={"patch_plan": patch_plan},
        )
    )
    return candidates[:max_candidates]


def _run_single_cycle(
    *,
    run_dir: Path,
    input_path: Path,
    slice_n: int,
    unknown_rate_max: float,
    mode: str,
    max_candidates: int,
    stability_enabled: bool,
    stability_slice_n: int,
    val_inputs: list[Path],
    loop_index: int,
) -> tuple[bool, dict[str, Any]]:
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "RUN.log"

    _log("OPS cycle started", log_path)
    _log(f"input_csv={input_path}", log_path)
    _log(f"slice={slice_n}", log_path)

    if not input_path.exists():
        _log(f"ERROR: input CSV not found: {input_path}", log_path)
        return False, {
            "run_dir": str(run_dir),
            "input_csv": str(input_path),
            "error": f"input_not_found:{input_path}",
            "gate_details": {},
            "patch": {},
        }

    slice_csv = run_dir / "slice_input.csv"
    slice_rows = copy_slice(input_path, slice_csv, slice_n)
    _log(f"slice rows written: {slice_rows} -> {slice_csv}", log_path)
    theta_snapshot = get_theta_snapshot()
    theta_snapshot["ops_auto_noise_domains"] = sorted(_load_ops_auto_domains(ROOT / "src" / "filters.py"))
    theta_snapshot["runtime"] = {
        "slice_n": slice_n,
        "mode": mode,
        "max_candidates": max_candidates,
        "unknown_rate_max": unknown_rate_max,
        "stability_enabled": stability_enabled,
        "stability_slice_n": stability_slice_n,
        "val_inputs": [str(p) for p in val_inputs],
        "deterministic_slice": True,
    }
    theta_hash = hashlib.sha256(json.dumps(theta_snapshot, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:12]
    _log(f"theta_hash={theta_hash}", log_path)

    before_json = run_dir / "BEFORE_KPI.json"
    before_md = run_dir / "BEFORE_KPI_REPORT.md"
    before = kpi_generate.run(slice_csv, before_json, before_md, slice_value=None)
    before = _inject_theta_snapshot(before_json, before, theta_snapshot)
    theta_before_payload = compute_progress_theta(before)
    _log(f"before KPI generated: {before_json}", log_path)

    candidates = _build_mode_b_candidates(before, max_candidates=max_candidates) if mode == "B" else [
        Candidate(
            kind="domain_block",
            name="A:domain_block_from_kpi",
            payload={"patch_plan": propose_patch(ROOT, before, max_domains=5)},
        )
    ]
    _log(f"candidate_count={len(candidates)} mode={mode}", log_path)

    patch_result = PatchResult(
        applied=False,
        target_file=ROOT / "src" / "filters.py",
        backup_file=None,
        diff_text="",
        domains_added=[],
        message="patch_empty:no_candidate_attempted",
    )
    patch_plan = PatchPlan(
        target_file=ROOT / "src" / "filters.py",
        candidate_domains=[],
        reason="none",
    )
    selected_candidate: Candidate | None = None
    attempt_records: list[dict[str, Any]] = []
    passed = False
    mandatory_gates: dict[str, bool] = {}
    advisory_gates: dict[str, bool] = {}
    gate_details: dict[str, Any] = {}
    reverted = False
    after: dict[str, Any] = before

    after_slice_csv = run_dir / "after_slice_filtered.csv"
    after_json = run_dir / "AFTER_KPI.json"
    after_md = run_dir / "AFTER_KPI_REPORT.md"
    patch_diff_path = run_dir / "PATCH.diff"
    stability: dict[str, Any] = {
        "ran": False,
        "passed": (not stability_enabled),
        "slice_n": stability_slice_n,
    }
    val_actions: dict[str, Any] = {
        "enabled": bool(val_inputs),
        "all_passed": (not bool(val_inputs)),
        "slice_n": stability_slice_n if stability_slice_n > 0 else slice_n,
        "results": [],
    }
    theta_after_payload = dict(theta_before_payload)
    theta_has_improved = False

    for i, candidate in enumerate(candidates, start=1):
        if candidate.kind == "domain_block":
            patch_plan = candidate.payload["patch_plan"]
            patch_result = apply_patch(patch_plan, run_dir)
        elif candidate.kind == "unknown_marker":
            term = str(candidate.payload["term"])
            patch_result = _apply_unknown_marker_candidate(term, run_dir)
            patch_plan = PatchPlan(
                target_file=ROOT / "tools" / "kpi_generate.py",
                candidate_domains=[],
                reason=f"mode_b_priority_b:{term}",
            )
        elif candidate.kind == "corporate_keyword":
            term = str(candidate.payload["term"])
            patch_result = _apply_corporate_keyword_candidate(term, run_dir)
            patch_plan = PatchPlan(
                target_file=ROOT / "src" / "filters.py",
                candidate_domains=[],
                reason=f"mode_b_priority_a:{term}",
            )
        else:
            continue

        attempt_diff = run_dir / f"ATTEMPT_{i:02d}.diff"
        attempt_diff.write_text(patch_result.diff_text, encoding="utf-8")
        _log(
            f"attempt={i} candidate={candidate.name} applied={patch_result.applied} message={patch_result.message}",
            log_path,
        )
        if not patch_result.applied:
            attempt_records.append(
                {
                    "attempt": i,
                    "candidate": candidate.name,
                    "applied": False,
                    "message": patch_result.message,
                    "passed": False,
                }
            )
            continue

        before_rows, kept_rows = build_after_slice_with_current_filter(slice_csv, after_slice_csv)
        _log(f"after filter applied to slice: kept={kept_rows}/{before_rows} -> {after_slice_csv}", log_path)
        after = kpi_generate.run(after_slice_csv, after_json, after_md, slice_value=None)
        after = _inject_theta_snapshot(after_json, after, theta_snapshot)
        _append_after_kpi_delta_report(after_md, before, after)
        _log(f"after KPI generated: {after_json}", log_path)
        theta_after_payload = compute_progress_theta(after)
        theta_has_improved = theta_improved(theta_before_payload["theta"], theta_after_payload["theta"])

        passed, mandatory_gates, advisory_gates, gate_details = gate_result(
            before,
            after,
            unknown_rate_max,
            gate_policy=theta_snapshot.get("gate_policy", {}),
        )
        attempt_records.append(
            {
                "attempt": i,
                "candidate": candidate.name,
                "applied": True,
                "message": patch_result.message,
                "mandatory_gates": mandatory_gates,
                "advisory_gates": advisory_gates,
                "passed": passed,
            }
        )
        gate_details["theta_before"] = theta_before_payload["theta"]
        gate_details["theta_after"] = theta_after_payload["theta"]
        gate_details["theta_improved"] = theta_has_improved
        if passed:
            if _should_run_stability(True, stability_enabled):
                stability = _run_stability_check(
                    run_dir=run_dir,
                    input_path=input_path,
                    stability_slice_n=stability_slice_n,
                    theta_snapshot=theta_snapshot,
                    unknown_rate_max=unknown_rate_max,
                    log_path=log_path,
                )
                attempt_records[-1]["stability"] = {
                    "ran": True,
                    "passed": bool(stability.get("passed")),
                }
                if not bool(stability.get("passed")):
                    mandatory_gates = _merge_stability_gate(mandatory_gates, stability_enabled, False)
                    passed = False
                    gate_details["gate_reasons"] = list(gate_details.get("gate_reasons", [])) + ["failed:stability_passed"]
                    reverted = revert_patch(patch_result)
                    _log(f"stability failed on attempt={i} -> reverted={reverted}", log_path)
                    continue

            mandatory_gates = _merge_stability_gate(mandatory_gates, stability_enabled, True if stability_enabled else None)
            if val_inputs:
                val_actions = _run_validation_gate(
                    run_dir=run_dir,
                    val_inputs=val_inputs,
                    input_slice_n=stability_slice_n if stability_slice_n > 0 else slice_n,
                    theta_snapshot=theta_snapshot,
                    theta_hash=theta_hash,
                    unknown_rate_max=unknown_rate_max,
                    log_path=log_path,
                )
                attempt_records[-1]["val"] = {
                    "enabled": True,
                    "all_passed": bool(val_actions.get("all_passed")),
                }
                if not bool(val_actions.get("all_passed")):
                    mandatory_gates = _merge_val_gate(mandatory_gates, True, False)
                    passed = False
                    gate_details["gate_reasons"] = list(gate_details.get("gate_reasons", [])) + ["failed:val_passed"]
                    reverted = revert_patch(patch_result)
                    _log(f"val gate failed on attempt={i} -> reverted={reverted}", log_path)
                    continue
            mandatory_gates = _merge_val_gate(mandatory_gates, bool(val_inputs), True if val_inputs else None)
            accepted, theta_has_improved = should_accept_patch(
                mandatory_gates=mandatory_gates,
                theta_before=theta_before_payload["theta"],
                theta_after=theta_after_payload["theta"],
            )
            mandatory_gates["theta_improved"] = theta_has_improved
            gate_details["theta_improved"] = theta_has_improved
            if not accepted:
                passed = False
                gate_details["gate_reasons"] = list(gate_details.get("gate_reasons", [])) + ["failed:theta_improved"]
                reverted = revert_patch(patch_result)
                _log(f"theta did not improve on attempt={i} -> reverted={reverted}", log_path)
                continue
            selected_candidate = candidate
            patch_diff_path.write_text(patch_result.diff_text, encoding="utf-8")
            _log(f"gates passed on attempt={i} -> patch kept", log_path)
            break

        reverted = revert_patch(patch_result)
        _log(f"gates failed on attempt={i} -> reverted={reverted}", log_path)

    if not patch_diff_path.exists():
        patch_diff_path.write_text(patch_result.diff_text, encoding="utf-8")

    if not gate_details:
        shutil.copyfile(before_json, after_json)
        shutil.copyfile(before_md, after_md)
        after = _inject_theta_snapshot(after_json, before, theta_snapshot)
        theta_after_payload = compute_progress_theta(after)
        theta_has_improved = False
        passed, mandatory_gates, advisory_gates, gate_details = gate_result(
            before,
            after,
            unknown_rate_max,
            gate_policy=theta_snapshot.get("gate_policy", {}),
        )
        mandatory_gates = _merge_stability_gate(mandatory_gates, stability_enabled, False if stability_enabled else None)
        mandatory_gates = _merge_val_gate(mandatory_gates, bool(val_inputs), False if val_inputs else None)
        mandatory_gates["theta_improved"] = False

    if not attempt_records:
        # Keep output contract even when no attempt was possible.
        shutil.copyfile(before_json, after_json)
        shutil.copyfile(before_md, after_md)
        after = _inject_theta_snapshot(after_json, before, theta_snapshot)
        theta_after_payload = compute_progress_theta(after)
        theta_has_improved = False
        passed, mandatory_gates, advisory_gates, gate_details = gate_result(
            before,
            after,
            unknown_rate_max,
            gate_policy=theta_snapshot.get("gate_policy", {}),
        )
        mandatory_gates = _merge_stability_gate(mandatory_gates, stability_enabled, False if stability_enabled else None)
        mandatory_gates = _merge_val_gate(mandatory_gates, bool(val_inputs), False if val_inputs else None)
        mandatory_gates["theta_improved"] = False
        _log("no candidate attempts were made", log_path)

    gate_details["theta_before"] = theta_before_payload["theta"]
    gate_details["theta_after"] = theta_after_payload["theta"]
    gate_details["theta_improved"] = theta_has_improved

    if val_inputs and not (run_dir / "VAL_ACTIONS.json").exists():
        _write_val_artifacts(run_dir, val_actions)

    theta_after_snapshot = get_theta_snapshot()
    theta_after_snapshot["ops_auto_noise_domains"] = sorted(_load_ops_auto_domains(ROOT / "src" / "filters.py"))
    theta_after_snapshot["runtime"] = dict(theta_snapshot.get("runtime", {}))
    theta_after_hash = hashlib.sha256(
        json.dumps(theta_after_snapshot, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:12]
    failing_gate_key = _failing_gate_key(mandatory_gates)

    actions = {
        "loop_index": loop_index,
        "run_dir": str(run_dir),
        "input_csv": str(input_path),
        "slice_csv": str(slice_csv),
        "after_slice_csv": str(after_slice_csv),
        "before_kpi": str(before_json),
        "after_kpi": str(after_json),
        "before_report": str(before_md),
        "after_report": str(after_md),
        "patch_diff": str(patch_diff_path),
        "mode": mode,
        "unknown_rate_max": unknown_rate_max,
        "stability_enabled": stability_enabled,
        "stability_slice_n": stability_slice_n,
        "val_inputs": [str(p) for p in val_inputs],
        "theta_hash": theta_hash,
        "theta_before": theta_hash,
        "theta_after": theta_after_hash,
        "progress_theta": {
            "before": theta_before_payload,
            "after": theta_after_payload,
        },
        "theta_before_score": theta_before_payload["theta"],
        "theta_after_score": theta_after_payload["theta"],
        "theta_improved": theta_has_improved,
        "theta_snapshot": theta_snapshot,
        "patch": {
            "target_file": str(patch_plan.target_file),
            "domains": patch_plan.candidate_domains,
            "applied": patch_result.applied,
            "message": patch_result.message,
            "backup_file": str(patch_result.backup_file) if patch_result.backup_file else None,
            "selected_candidate": selected_candidate.name if selected_candidate else None,
        },
        "mandatory_gates": mandatory_gates,
        "advisory_gates": advisory_gates,
        "gate_details": gate_details,
        "before_top50_noise_count": gate_details.get("before_top50_noise_count"),
        "after_top50_noise_count": gate_details.get("after_top50_noise_count"),
        "before_top50_good_count": gate_details.get("before_top50_good_count"),
        "after_top50_good_count": gate_details.get("after_top50_good_count"),
        "top50_good_drop_explained": gate_details.get("top50_good_drop_explained"),
        "failing_gate_key": failing_gate_key,
        "gate_reason": (gate_details.get("gate_reasons", [""])[0] if gate_details.get("gate_reasons") else ""),
        "gate_reasons": gate_details.get("gate_reasons", []),
        "stability_passed": bool(stability.get("passed", not stability_enabled)),
        "stability": stability,
        "val": val_actions,
        "attempts": attempt_records,
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
        patch_target_file=patch_plan.target_file,
        candidate_domains=patch_plan.candidate_domains,
        patch_applied=patch_result.applied,
        patch_message=patch_result.message,
        gates_passed=passed,
        mandatory_gates=mandatory_gates,
        advisory_gates=advisory_gates,
        gate_details=gate_details,
        theta_hash=theta_hash,
        reverted=reverted,
    )
    _log("OPS cycle completed", log_path)
    _log(f"artifacts: {run_dir}", log_path)
    return passed, actions


def _write_final_ruleset(out_path: Path) -> None:
    filters_path = ROOT / "src" / "filters.py"
    ops_auto_domains = sorted(_load_ops_auto_domains(filters_path))
    corporate_keywords = _load_corporate_keywords_from_filters(filters_path)
    lines = [
        "# FINAL_RULESET",
        "",
        "## src/filters.py",
        "",
        "### CORPORATE_KEYWORDS",
        "",
    ]
    lines.extend([f"- {k}" for k in corporate_keywords])
    lines.extend(["", "### OPS_AUTO_EXCLUDED_DOMAINS", ""])
    lines.extend([f"- {d}" for d in ops_auto_domains])
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_loops(
    input_path: Path,
    slice_n: int,
    loop_n: int,
    mode: str,
    max_candidates: int,
    unknown_rate_max: float,
    stability_enabled: bool,
    stability_slice_n: int,
    val_inputs: list[Path],
    no_progress_k: int,
) -> int:
    root_dir = OPS_RUNS_DIR / _ts()
    root_dir.mkdir(parents=True, exist_ok=True)
    loop_summary = [
        "# LOOP_SUMMARY",
        "",
        f"- mode: `{mode}`",
        f"- loops: `{loop_n}`",
        f"- max_candidates: `{max_candidates}`",
        f"- stability_enabled: `{stability_enabled}`",
        f"- stability_slice_n: `{stability_slice_n}`",
        f"- val_inputs: `{', '.join(str(p) for p in val_inputs) if val_inputs else '(none)'}`",
        f"- no_progress_k: `{no_progress_k}`",
        "",
    ]
    exit_code = 0
    no_progress_streak = 0

    for i in range(1, loop_n + 1):
        loop_dir = root_dir / f"loop_{i:02d}"
        passed, actions = _run_single_cycle(
            run_dir=loop_dir,
            input_path=input_path,
            slice_n=slice_n,
            unknown_rate_max=unknown_rate_max,
            mode=mode,
            max_candidates=max_candidates,
            stability_enabled=stability_enabled,
            stability_slice_n=stability_slice_n,
            val_inputs=val_inputs,
            loop_index=i,
        )
        gd = actions.get("gate_details", {})
        stability = actions.get("stability", {})
        val = actions.get("val", {})
        failing_gate = actions.get("failing_gate_key", "")
        theta_before_score = actions.get("theta_before_score")
        theta_after_score = actions.get("theta_after_score")
        theta_loop_improved = bool(actions.get("theta_improved", False))
        no_progress_streak = _next_no_progress_streak(theta_loop_improved, no_progress_streak)
        loop_summary.extend(
            [
                f"## loop_{i:02d}",
                f"- run_dir: `{loop_dir}`",
                f"- passed: `{passed}`",
                f"- failing_gate_key: `{failing_gate or '-'}`",
                f"- gate_reasons: `{', '.join(actions.get('gate_reasons', []))}`",
                f"- selected_candidate: `{actions.get('patch', {}).get('selected_candidate')}`",
                f"- theta: `{theta_before_score}` -> `{theta_after_score}` improved=`{theta_loop_improved}`",
                f"- bad_domain_mix: `{gd.get('before_bad_domain_mix')}` -> `{gd.get('after_bad_domain_mix')}`",
                f"- top50_effective_good_count: `{gd.get('before_top50_effective_good_count')}` -> `{gd.get('after_top50_effective_good_count')}`",
                f"- after_unknown_rate: `{gd.get('after_unknown_rate')}` (max `{gd.get('unknown_rate_max')}`)",
                f"- stability: ran=`{stability.get('ran')}` passed=`{stability.get('passed')}` report=`{stability.get('report')}`",
                f"- val: enabled=`{val.get('enabled')}` all_passed=`{val.get('all_passed')}`",
                f"- no_progress_streak: `{no_progress_streak}`",
                "",
            ]
        )
        if no_progress_streak >= no_progress_k:
            next_action_path = _write_next_action(root_dir, failing_gate, mode)
            loop_summary.extend(
                [
                    "## stop_condition",
                    f"- reason: `no_progress_k_reached`",
                    f"- failing_gate_key: `{failing_gate}`",
                    f"- theta_improved: `{theta_loop_improved}`",
                    f"- next_action: `{next_action_path}`",
                    "",
                ]
            )
            exit_code = 2
            break
        if not passed:
            exit_code = 1

    (root_dir / "LOOP_SUMMARY.md").write_text("\n".join(loop_summary), encoding="utf-8")
    _write_final_ruleset(root_dir / "FINAL_RULESET.md")
    print(f"[INFO] loop artifacts root: {root_dir}")
    return exit_code


def run_cycle(
    input_path: Path,
    slice_n: int,
    mode: str,
    loop_n: int,
    max_candidates: int,
    unknown_rate_max: float,
    stability_enabled: bool,
    stability_slice_n: int,
    val_inputs: list[Path],
    no_progress_k: int,
) -> int:
    return run_loops(
        input_path=input_path,
        slice_n=slice_n,
        loop_n=loop_n,
        mode=mode,
        max_candidates=max_candidates,
        unknown_rate_max=unknown_rate_max,
        stability_enabled=stability_enabled,
        stability_slice_n=stability_slice_n,
        val_inputs=val_inputs,
        no_progress_k=no_progress_k,
    )


def main() -> int:
    args = parse_args()
    if args.slice <= 0:
        print("ERROR: --slice must be > 0")
        return 2
    if args.loop <= 0:
        print("ERROR: --loop must be > 0")
        return 2
    if args.max_candidates <= 0:
        print("ERROR: --max-candidates must be > 0")
        return 2
    if args.unknown_rate_max <= 0 or args.unknown_rate_max > 1:
        print("ERROR: --unknown-rate-max must be in (0, 1]")
        return 2
    if args.stability_slice <= 0:
        print("ERROR: --stability-slice must be > 0")
        return 2
    if args.no_progress_k <= 0:
        print("ERROR: --no-progress-k must be > 0")
        return 2
    val_inputs = _resolve_val_inputs(args.val_inputs, args.val_manifest)
    input_path = Path(args.input)
    return run_cycle(
        input_path=input_path,
        slice_n=args.slice,
        mode=args.mode,
        loop_n=args.loop,
        max_candidates=args.max_candidates,
        unknown_rate_max=args.unknown_rate_max,
        stability_enabled=not args.no_stability,
        stability_slice_n=args.stability_slice,
        val_inputs=val_inputs,
        no_progress_k=args.no_progress_k,
    )


if __name__ == "__main__":
    raise SystemExit(main())
