#!/usr/bin/env python3
"""
Minimal precision-first patch engine for OPS cycles.

The engine proposes a deterministic domain-noise patch from KPI diagnostics,
applies it to src/filters.py, and can revert from a backup.
"""

from __future__ import annotations

import difflib
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any


OPS_BLOCK_START = "# OPS_AUTO_BLOCKLIST_START"
OPS_BLOCK_END = "# OPS_AUTO_BLOCKLIST_END"

CHECK_SNIPPET = """    if domain in OPS_AUTO_EXCLUDED_DOMAINS:
        return True, f'excluded_domain:ops_auto:{domain}'

    for excluded in OPS_AUTO_EXCLUDED_DOMAINS:
        if domain.endswith('.' + excluded) or domain == excluded:
            return True, f'excluded_domain:ops_auto:{excluded}'

"""


@dataclass
class PatchPlan:
    target_file: Path
    candidate_domains: list[str]
    reason: str


@dataclass
class PatchResult:
    applied: bool
    target_file: Path
    backup_file: Path | None
    diff_text: str
    domains_added: list[str]
    message: str


def _normalize_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    d = d.removeprefix("https://").removeprefix("http://")
    d = d.split("/", 1)[0]
    d = d.removeprefix("www.")
    return d


def select_candidate_domains(kpi: dict[str, Any], max_domains: int = 5) -> list[str]:
    """Pick deterministic bad domains from KPI diagnostics."""
    diagnostics = kpi.get("diagnostics", {}) if isinstance(kpi, dict) else {}
    bad_domains = diagnostics.get("bad_domains_top", [])
    ranked: list[tuple[int, str]] = []
    for item in bad_domains:
        if not isinstance(item, dict):
            continue
        domain = _normalize_domain(str(item.get("domain", "")))
        if not domain or "." not in domain:
            continue
        count = int(item.get("count", 0) or 0)
        ranked.append((count, domain))

    ranked.sort(key=lambda x: (-x[0], x[1]))
    deduped: list[str] = []
    for _, domain in ranked:
        if domain not in deduped:
            deduped.append(domain)
        if len(deduped) >= max_domains:
            break
    return deduped


def propose_patch(repo_root: Path, before_kpi: dict[str, Any], max_domains: int = 5) -> PatchPlan:
    target = repo_root / "src" / "filters.py"
    domains = select_candidate_domains(before_kpi, max_domains=max_domains)
    reason = "precision-first: expand domain noise exclusions from KPI diagnostics"
    return PatchPlan(target_file=target, candidate_domains=domains, reason=reason)


def _build_ops_block(domains: list[str]) -> str:
    domain_lines = "\n".join(f"    '{d}'," for d in domains)
    return (
        f"{OPS_BLOCK_START}\n"
        "OPS_AUTO_EXCLUDED_DOMAINS = {\n"
        f"{domain_lines}\n"
        "}\n"
        f"{OPS_BLOCK_END}\n"
    )


def _upsert_ops_block(text: str, domains: list[str]) -> str:
    block = _build_ops_block(domains)
    pattern = re.compile(
        rf"{re.escape(OPS_BLOCK_START)}.*?{re.escape(OPS_BLOCK_END)}\n?",
        re.DOTALL,
    )
    if pattern.search(text):
        return pattern.sub(block, text)

    suffix = "" if text.endswith("\n") else "\n"
    return f"{text}{suffix}\n{block}"


def _insert_check_snippet_if_missing(text: str) -> str:
    if "excluded_domain:ops_auto" in text:
        return text

    anchor = "    if not domain:\n        return False, ''\n\n"
    if anchor in text:
        return text.replace(anchor, f"{anchor}{CHECK_SNIPPET}", 1)

    # Fallback no-op if function anchor is not found.
    return text


def apply_patch_to_file(target_file: Path, domains: list[str], run_dir: Path) -> PatchResult:
    """
    Apply deterministic OPS patch to a target filter file.

    The patch adds/updates OPS_AUTO_EXCLUDED_DOMAINS and injects a small check
    in is_excluded_domain().
    """
    if not target_file.exists():
        return PatchResult(
            applied=False,
            target_file=target_file,
            backup_file=None,
            diff_text="",
            domains_added=[],
            message=f"target file missing: {target_file}",
        )

    domains = [_normalize_domain(d) for d in domains]
    domains = [d for d in domains if d and "." in d]
    domains = sorted(set(domains))
    if not domains:
        return PatchResult(
            applied=False,
            target_file=target_file,
            backup_file=None,
            diff_text="",
            domains_added=[],
            message="no candidate domains from KPI diagnostics",
        )

    original = target_file.read_text(encoding="utf-8", errors="replace")
    updated = _insert_check_snippet_if_missing(original)
    updated = _upsert_ops_block(updated, domains)

    if updated == original:
        return PatchResult(
            applied=False,
            target_file=target_file,
            backup_file=None,
            diff_text="",
            domains_added=domains,
            message="patch produced no content change",
        )

    run_dir.mkdir(parents=True, exist_ok=True)
    backup = run_dir / f"{target_file.name}.bak"
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
        domains_added=domains,
        message="applied",
    )


def apply_patch(plan: PatchPlan, run_dir: Path) -> PatchResult:
    return apply_patch_to_file(plan.target_file, plan.candidate_domains, run_dir)


def revert_patch(result: PatchResult) -> bool:
    """Revert target from backup if patch was applied."""
    if not result.applied:
        return False
    if not result.backup_file or not result.backup_file.exists():
        return False
    shutil.copyfile(result.backup_file, result.target_file)
    return True

