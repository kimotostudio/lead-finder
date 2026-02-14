import csv
import json
from pathlib import Path

from tools.kpi_generate import run


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        raise ValueError("rows must not be empty")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_noise_excluded_from_positive_leads(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    kpi_path = tmp_path / "KPI.json"
    report_path = tmp_path / "KPI_REPORT.md"

    rows = [
        {"URL": "https://weather.com/jp", "リードスコア": "100", "個人度分類": "solo", "市区町村": "福岡市中央区", "営業優先度": "○"},
        {"URL": "https://my-salon.jp", "リードスコア": "90", "個人度分類": "solo", "市区町村": "福岡市南区", "営業優先度": "○"},
        {"URL": "https://my-salon2.jp", "リードスコア": "0", "個人度分類": "small", "市区町村": "福岡市博多区", "営業優先度": "△"},
    ]
    _write_csv(csv_path, rows)

    kpi = run(csv_path, kpi_path, report_path, slice_value=None)
    assert kpi["counts"]["total_leads"] == 3
    assert kpi["counts"]["noise_leads"] == 1
    # weather.com row has score>0 but must be excluded from positive_leads.
    assert kpi["counts"]["positive_leads"] == 1


def test_bad_domain_mix_definition(tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    kpi_path = tmp_path / "KPI.json"
    report_path = tmp_path / "KPI_REPORT.md"

    rows = [
        {"URL": "https://weather.com/jp", "リードスコア": "10"},
        {"URL": "https://xvideos.com/abc", "リードスコア": "10"},
        {"URL": "https://local-salon.jp", "リードスコア": "10"},
        {"URL": "https://local-2.jp", "リードスコア": "10"},
    ]
    _write_csv(csv_path, rows)

    kpi = run(csv_path, kpi_path, report_path, slice_value=None)
    assert kpi["counts"]["noise_leads"] == 2
    assert kpi["counts"]["total_leads"] == 4
    assert kpi["rates"]["bad_domain_mix"] == 0.5


def test_phase_complete_flips_when_thresholds_met(tmp_path: Path) -> None:
    good_csv = tmp_path / "good.csv"
    bad_csv = tmp_path / "bad.csv"
    good_kpi = tmp_path / "good_KPI.json"
    bad_kpi = tmp_path / "bad_KPI.json"
    good_report = tmp_path / "good_report.md"
    bad_report = tmp_path / "bad_report.md"

    good_rows = []
    for i in range(20):
        good_rows.append(
            {
                "URL": f"https://local-{i}.jp",
                "リードスコア": "90",
                "個人度分類": "solo",
                "市区町村": "福岡市中央区",
                "営業優先度": "○",
            }
        )
    _write_csv(good_csv, good_rows)
    kpi_good = run(good_csv, good_kpi, good_report, slice_value=None)
    assert kpi_good["phase"]["phase_complete"] is True
    assert kpi_good["phase"]["blocking_kpis"] == []

    bad_rows = []
    for i in range(10):
        bad_rows.append(
            {
                "URL": f"https://corp-{i}.jp",
                "リードスコア": "90",
                "個人度分類": "corporate",
                "市区町村": "福岡市中央区",
                "営業優先度": "△",
            }
        )
    _write_csv(bad_csv, bad_rows)
    kpi_bad = run(bad_csv, bad_kpi, bad_report, slice_value=None)
    assert kpi_bad["phase"]["phase_complete"] is False
    assert "solo_rate" in kpi_bad["phase"]["blocking_kpis"] or "corporate_rate" in kpi_bad["phase"]["blocking_kpis"]

    # Output JSON files are valid and stable.
    parsed = json.loads(good_kpi.read_text(encoding="utf-8"))
    assert parsed["schema_version"] == "1.0"
