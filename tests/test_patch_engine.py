from pathlib import Path

from tools.patch_engine import apply_patch_to_file, revert_patch, select_candidate_domains


def test_select_candidate_domains_deterministic() -> None:
    kpi = {
        "diagnostics": {
            "bad_domains_top": [
                {"domain": "B.com", "count": 1},
                {"domain": "a.com", "count": 2},
                {"domain": "a.com", "count": 9},  # duplicate domain ignored in final
                {"domain": "invalid", "count": 10},
            ]
        }
    }
    out = select_candidate_domains(kpi, max_domains=3)
    assert out == ["a.com", "b.com"]


def test_apply_and_revert_patch_to_file(tmp_path: Path) -> None:
    target = tmp_path / "filters.py"
    run_dir = tmp_path / "run"
    target.write_text(
        "EXCLUDED_DOMAINS = {'example.com'}\n\n"
        "def is_excluded_domain(url: str):\n"
        "    domain = 'a.com'\n"
        "    if not domain:\n"
        "        return False, ''\n\n"
        "    # Check exact match\n"
        "    if domain in EXCLUDED_DOMAINS:\n"
        "        return True, 'excluded'\n"
        "    return False, ''\n",
        encoding="utf-8",
    )

    result = apply_patch_to_file(target, ["noise.com", "news.example"], run_dir)
    assert result.applied is True
    updated = target.read_text(encoding="utf-8")
    assert "OPS_AUTO_EXCLUDED_DOMAINS" in updated
    assert "noise.com" in updated
    assert "excluded_domain:ops_auto" in updated
    assert result.backup_file is not None and result.backup_file.exists()

    assert revert_patch(result) is True
    reverted = target.read_text(encoding="utf-8")
    assert "OPS_AUTO_EXCLUDED_DOMAINS" not in reverted


def test_apply_patch_empty_when_candidate_domains_missing(tmp_path: Path) -> None:
    target = tmp_path / "filters.py"
    target.write_text("def is_excluded_domain(url: str):\n    return False, ''\n", encoding="utf-8")
    result = apply_patch_to_file(target, [], tmp_path / "run")
    assert result.applied is False
    assert result.message == "patch_empty:no_candidate_domains"
    assert result.diff_text == ""


def test_apply_patch_keeps_existing_ops_domains(tmp_path: Path) -> None:
    target = tmp_path / "filters.py"
    run_dir = tmp_path / "run"
    target.write_text(
        "# OPS_AUTO_BLOCKLIST_START\n"
        "OPS_AUTO_EXCLUDED_DOMAINS = {\n"
        "    'existing.com',\n"
        "}\n"
        "# OPS_AUTO_BLOCKLIST_END\n\n"
        "def is_excluded_domain(url: str):\n"
        "    domain = 'x'\n"
        "    if not domain:\n"
        "        return False, ''\n"
        "    return False, ''\n",
        encoding="utf-8",
    )
    result = apply_patch_to_file(target, ["new.com"], run_dir)
    assert result.applied is True
    updated = target.read_text(encoding="utf-8")
    assert "existing.com" in updated
    assert "new.com" in updated
