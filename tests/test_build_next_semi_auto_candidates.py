import csv
from pathlib import Path

from tools.build_next_semi_auto_candidates import build_candidates


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


def test_build_candidates_filters_feedback_and_quality_issues(tmp_path: Path) -> None:
    input_path = tmp_path / "input.csv"
    feedback_path = tmp_path / "feedback.csv"
    ledger_path = tmp_path / "ledger.csv"

    _write_csv(
        input_path,
        [
            {
                "lead_id": "good-1",
                "display_name": "Good Private Salon",
                "website": "https://good.example/contact",
                "contact_url": "https://good.example/contact",
                "score": "90",
                "name_confidence": "high",
                "original__has_contact_page": "true",
                "original__has_form": "true",
            },
            {
                "lead_id": "corp-1",
                "display_name": "Corp Salon Inc.",
                "website": "https://corp.example/contact",
                "contact_url": "https://corp.example/contact",
                "score": "90",
                "name_confidence": "high",
            },
            {
                "lead_id": "line-1",
                "display_name": "Line Salon",
                "website": "https://line-salon.example",
                "contact_url": "https://lin.ee/abc",
                "score": "90",
                "name_confidence": "high",
            },
            {
                "lead_id": "portal-1",
                "display_name": "Portal Salon",
                "website": "https://beauty.hotpepper.jp/slnH000",
                "score": "90",
                "name_confidence": "high",
            },
            {
                "lead_id": "low-name-1",
                "display_name": "福岡市 口コミ ランキング",
                "website": "https://low-name.example/contact",
                "contact_url": "https://low-name.example/contact",
                "score": "90",
                "name_confidence": "low",
            },
            {
                "lead_id": "duplicate-1",
                "display_name": "Duplicate Salon",
                "website": "https://duplicate.example/contact",
                "contact_url": "https://duplicate.example/contact",
                "score": "90",
                "name_confidence": "high",
            },
            {
                "lead_id": "feedback-bad-1",
                "display_name": "Feedback Bad Salon",
                "website": "https://feedback-bad.example/contact",
                "contact_url": "https://feedback-bad.example/contact",
                "score": "90",
                "name_confidence": "high",
            },
            {
                "lead_id": "domain-label-1",
                "display_name": "domain-label",
                "website": "https://domain-label.example/contact",
                "contact_url": "https://domain-label.example/contact",
                "score": "90",
                "name_confidence": "high",
            },
            {
                "lead_id": "reviewed-1",
                "display_name": "Reviewed Salon",
                "website": "https://reviewed.example/contact",
                "contact_url": "https://reviewed.example/contact",
                "score": "90",
                "name_confidence": "high",
            },
            {
                "lead_id": "medical-1",
                "display_name": "福岡天神の美容皮膚科",
                "website": "https://medical.example/contact",
                "contact_url": "https://medical.example/contact",
                "score": "90",
                "name_confidence": "high",
            },
            {
                "lead_id": "tel-1",
                "display_name": "Tel Only Salon",
                "website": "https://tel-only.example/",
                "contact_url": "tel:0920000000",
                "score": "90",
                "name_confidence": "high",
            },
        ],
    )
    _write_csv(
        feedback_path,
        [
            {
                "lead_id": "feedback-bad-1",
                "domain": "feedback-bad.example",
                "lead_finder_recommended_action": "exclude",
                "lead_finder_score_penalty": "-100",
                "lead_finder_exclusion_reason": "prior_bad_contact",
            }
        ],
    )
    _write_csv(
        ledger_path,
        [
            {
                "salon_id": "duplicate-1",
                "domain": "duplicate.example",
                "status": "prepared_full",
            }
        ],
    )
    review_queue_path = tmp_path / "review_queue.csv"
    _write_csv(
        review_queue_path,
        [
            {
                "salon_id": "reviewed-1",
                "domain": "reviewed.example",
                "status": "prepared_full",
                "reason": "prepared",
            }
        ],
    )

    selected, audit_rows, counts, fieldnames = build_candidates(
        input_path=input_path,
        feedback_path=feedback_path,
        ledger_path=ledger_path,
        review_queue_path=review_queue_path,
        min_name_confidence="medium",
        min_quality_score=50,
        limit=20,
    )

    assert [row["lead_id"] for row in selected] == ["good-1"]
    assert counts["input_rows"] == 11
    assert counts["excluded_corporate_like"] == 1
    assert counts["excluded_line_or_sns"] == 1
    assert counts["excluded_portal_listing"] == 1
    assert counts["excluded_low_name_confidence"] == 2
    assert counts["excluded_duplicate_ledger"] == 2
    assert counts["excluded_feedback_exclude"] == 1
    assert counts["excluded_medical_like"] == 1
    assert counts["excluded_weak_contact"] == 3
    assert counts["candidate_count"] == 1
    assert len(audit_rows) == 11
    assert "semi_auto_quality_score" in fieldnames


def test_build_candidates_can_require_location_token(tmp_path: Path) -> None:
    input_path = tmp_path / "input.csv"
    feedback_path = tmp_path / "feedback.csv"
    ledger_path = tmp_path / "ledger.csv"
    _write_csv(
        input_path,
        [
            {
                "lead_id": "fukuoka-1",
                "display_name": "福岡市 Private Salon",
                "website": "https://fukuoka.example/contact",
                "contact_url": "https://fukuoka.example/contact",
                "score": "90",
                "name_confidence": "high",
                "has_contact_page": "true",
            },
            {
                "lead_id": "nagoya-1",
                "display_name": "名古屋 Private Salon",
                "website": "https://nagoya.example/contact",
                "contact_url": "https://nagoya.example/contact",
                "score": "90",
                "name_confidence": "high",
                "has_contact_page": "true",
            },
        ],
    )
    _write_csv(feedback_path, [])
    _write_csv(ledger_path, [])

    selected, audit_rows, counts, fieldnames = build_candidates(
        input_path=input_path,
        feedback_path=feedback_path,
        ledger_path=ledger_path,
        min_name_confidence="medium",
        min_quality_score=50,
        required_location_tokens=("福岡",),
        limit=20,
    )

    assert [row["lead_id"] for row in selected] == ["fukuoka-1"]
    assert counts["excluded_location_mismatch"] == 1
    assert [row["location_match"] for row in audit_rows] == ["1", "0"]
    assert "semi_auto_location_match" in fieldnames


def test_build_candidates_uses_blocklist_cooldowns_and_feedback_summary(tmp_path: Path) -> None:
    input_path = tmp_path / "input.csv"
    feedback_path = tmp_path / "lead_quality_feedback_latest.csv"
    ledger_path = tmp_path / "ledger.csv"
    blocklist_path = tmp_path / "blocklist_domains.txt"
    cooldowns_path = tmp_path / "domain_cooldowns.json"

    _write_csv(
        input_path,
        [
            {
                "lead_id": "ok-1",
                "display_name": "OK Salon",
                "website": "https://ok.example/",
                "contact_url": "https://ok.example/contact/",
                "score": "70",
                "name_confidence": "high",
                "original__has_contact_page": "true",
                "original__has_form": "true",
            },
            {
                "lead_id": "toc-1",
                "display_name": "Toc Salon",
                "website": "https://toc.example/",
                "contact_url": "https://toc.example/#toc3",
                "score": "70",
                "name_confidence": "high",
            },
            {
                "lead_id": "blocked-1",
                "display_name": "Blocked Salon",
                "website": "https://blocked.example/contact/",
                "contact_url": "https://blocked.example/contact/",
                "score": "70",
                "name_confidence": "high",
            },
            {
                "lead_id": "cooldown-1",
                "display_name": "Cooldown Salon",
                "website": "https://cooldown.example/contact/",
                "contact_url": "https://cooldown.example/contact/",
                "score": "70",
                "name_confidence": "high",
            },
            {
                "lead_id": "feedback-weak-1",
                "display_name": "Feedback Weak Salon",
                "website": "https://feedback.example/contact/",
                "contact_url": "https://feedback.example/contact/",
                "score": "70",
                "name_confidence": "high",
            },
        ],
    )
    _write_csv(
        feedback_path,
        [
            {
                "lead_id": "feedback-weak-1",
                "domain": "feedback.example",
                "recommended_action": "improve_contact_url",
                "failure_category": "weak_contact_url",
                "lead_selection_bonus": "10",
                "lead_selection_penalty": "25",
            }
        ],
    )
    _write_csv(ledger_path, [])
    blocklist_path.write_text("blocked.example\n", encoding="utf-8")
    cooldowns_path.write_text(
        '{"cooldown.example": {"until": "2999-01-01T00:00:00+09:00", "reason": "bot_protection"}}',
        encoding="utf-8",
    )

    selected, audit_rows, counts, fieldnames = build_candidates(
        input_path=input_path,
        feedback_path=feedback_path,
        ledger_path=ledger_path,
        blocklist_path=blocklist_path,
        cooldowns_path=cooldowns_path,
        min_name_confidence="medium",
        min_quality_score=50,
        limit=20,
    )

    assert [row["lead_id"] for row in selected] == ["ok-1", "feedback-weak-1"]
    assert counts["excluded_weak_contact"] == 1
    assert counts["excluded_blocklist_domain"] == 1
    assert counts["excluded_cooldown_domain"] == 1
    audit_by_id = {row["lead_id"]: row for row in audit_rows}
    assert "toc_anchor_contact" in audit_by_id["toc-1"]["quality_issue"]
    assert audit_by_id["feedback-weak-1"]["failure_category"] == "weak_contact_url"
    assert audit_by_id["feedback-weak-1"]["feedback_penalty"] == "25"
    assert "lead_selection_penalty" in fieldnames
