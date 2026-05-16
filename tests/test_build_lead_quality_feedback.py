import csv
from pathlib import Path

from tools.build_lead_quality_feedback import build_feedback


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_build_feedback_classifies_prepared_manual_and_blocked(tmp_path: Path) -> None:
    source_path = tmp_path / "handoff.csv"
    results_dir = tmp_path / "results"
    ledger_path = tmp_path / "ledger.csv"
    blocklist_path = tmp_path / "blocklist_domains.txt"
    cooldowns_path = tmp_path / "domain_cooldowns.json"

    _write_csv(
        source_path,
        [
            {
                "lead_id": "good-1",
                "domain": "good.example",
                "display_name": "Good Salon",
                "name_confidence": "high",
                "contact_url": "https://good.example/contact/",
                "website": "https://good.example/",
                "demo_url": "https://demo.example/good",
                "score": "80",
            },
            {
                "lead_id": "toc-1",
                "domain": "toc.example",
                "display_name": "Toc Salon",
                "name_confidence": "medium",
                "contact_url": "https://toc.example/#toc3",
                "website": "https://toc.example/",
                "score": "70",
            },
            {
                "lead_id": "blocked-1",
                "domain": "blocked.example",
                "display_name": "Blocked Salon",
                "name_confidence": "medium",
                "contact_url": "https://blocked.example/contact/",
                "website": "https://blocked.example/",
                "score": "70",
            },
        ],
    )
    _write_csv(
        results_dir / "lead_quality_feedback_20260101.csv",
        [
            {
                "lead_id": "good-1",
                "domain": "good.example",
                "final_status": "prepared_full",
                "reason": "prepared",
                "contact_quality_issue": "contact_ok",
                "form_quality_issue": "form_ok",
            },
            {
                "lead_id": "toc-1",
                "domain": "toc.example",
                "final_status": "prepared_review_needed",
                "reason": "no_form_fields",
                "form_quality_issue": "no_form_fields",
            },
        ],
    )
    _write_csv(
        results_dir / "submissions_20260101.csv",
        [
            {
                "timestamp": "2026-01-01 10:00:00",
                "salon_id": "blocked-1",
                "domain": "blocked.example",
                "status": "skipped_bot_protection",
                "message": "blocked_domain:blocked.example",
            }
        ],
    )
    _write_csv(ledger_path, [])
    blocklist_path.write_text("blocked.example\n", encoding="utf-8")
    cooldowns_path.write_text("{}", encoding="utf-8")

    rows = build_feedback(
        source_paths=[source_path],
        results_dir=results_dir,
        ledger_path=ledger_path,
        blocklist_path=blocklist_path,
        cooldowns_path=cooldowns_path,
        run_date="2026-01-01",
    )

    by_id = {row["lead_id"]: row for row in rows}
    assert by_id["good-1"]["prepared_success"] == "1"
    assert by_id["good-1"]["recommended_action"] == "prioritize_similar"
    assert by_id["good-1"]["outcome"] == "unknown"
    assert by_id["toc-1"]["failure_category"] == "no_form_fields"
    assert by_id["toc-1"]["recommended_action"] == "improve_contact_url"
    assert by_id["blocked-1"]["failure_category"] == "blocked_domain"
    assert by_id["blocked-1"]["recommended_action"] == "block"
