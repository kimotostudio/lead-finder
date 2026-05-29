#!/usr/bin/env python3
"""Build lead-finder quality feedback from local SEMI_AUTO artifacts.

This script is local-only. It reads handoff/source CSVs and Playwright
artifacts, then writes a reviewable feedback CSV for future scoring and
candidate selection. It does not open websites, run Playwright, submit forms,
or infer sales conversions.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


JST = ZoneInfo("Asia/Tokyo")
ROOT = Path(__file__).resolve().parents[1]
PLAYWRIGHT_ROOT = ROOT.parent / "playwright-automation"
DEFAULT_RESULTS_DIR = PLAYWRIGHT_ROOT / "results"
DEFAULT_LEDGER = PLAYWRIGHT_ROOT / "data" / "submission_ledger.csv"
DEFAULT_BLOCKLIST = PLAYWRIGHT_ROOT / "data" / "blocklist_domains.txt"
DEFAULT_COOLDOWNS = PLAYWRIGHT_ROOT / "data" / "domain_cooldowns.json"
DEFAULT_OUTPUT = ROOT / "ops_runs" / "lead_quality_feedback_latest.csv"
DEFAULT_SOURCE = ROOT.parent / "demo-generator" / "output" / "handoff_with_demo_paths.csv"

OUTPUT_FIELDS = [
    "lead_id",
    "domain",
    "display_name",
    "name_confidence",
    "name_source",
    "name_warning",
    "area",
    "business_type",
    "category",
    "original_score",
    "solo_score",
    "contact_url",
    "website",
    "demo_url",
    "semi_auto_status",
    "semi_auto_reason",
    "prepared_success",
    "manual_review_needed",
    "blocked_or_skipped",
    "failure_category",
    "recommended_action",
    "lead_selection_bonus",
    "lead_selection_penalty",
    "outcome",
    "outcome_source",
    "run_date",
]

ID_FIELDS = ("lead_id", "id", "salon_id", "ID", "管理番号")
DOMAIN_FIELDS = ("domain", "original__domain")
DISPLAY_FIELDS = ("display_name", "business_name", "salon_name", "brand_name", "company_name", "店名", "title")
CONTACT_FIELDS = ("contact_url", "contact_page", "original__contact_url", "original__form_url")
WEBSITE_FIELDS = ("website", "url", "reference_url", "original__url", "url(旧)")
DEMO_FIELDS = ("demo_url", "url(デモ)")
AREA_FIELDS = ("area", "location", "area_guess", "original__area_guess", "original__address")
TYPE_FIELDS = ("business_type", "industry")
CATEGORY_FIELDS = ("category", "category_guess", "original__category_guess", "industry", "business_type")
SCORE_FIELDS = ("score", "lead_score", "リードスコア", "original__score")
SOLO_FIELDS = ("solo_score", "original__solo_score", "個人度スコア(raw)", "個人度スコア(0-100)")

CONTACT_PATH_TOKENS = (
    "contact",
    "inquiry",
    "form",
    "reserve",
    "reservation",
    "booking",
    "otoiawase",
    "toiawase",
    "お問い合わせ",
    "問い合わせ",
    "予約",
    "ご相談",
)
EXTERNAL_RESERVATION_TOKENS = (
    "external_reservation",
    "reserva",
    "airrsv",
    "coubic",
    "reservestock",
    "salondenet",
    "select-type",
    "tol-app",
    "reservation.",
    "/reserve",
    "予約システム",
)
MEDIA_OR_LISTING_TOKENS = (
    "listing_or_media_form",
    "operator_contact_form",
    "unsuitable_contact_target",
    "portal",
    "listing",
    "directory",
    "media",
    "掲載",
    "広告",
    "媒体",
    "ポータル",
)
CORPORATE_OR_PORTAL_TOKENS = (
    "corporate_or_large_business",
    "corporate",
    "法人",
    "株式会社",
    "inc.",
    "corporation",
    "portal_listing",
    "hotpepper",
    "findglocal",
)


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def norm_key(value: str) -> str:
    text = str(value or "").replace("\ufeff", "").strip().lower()
    text = text.replace("（", "(").replace("）", ")").replace("　", " ")
    return re.sub(r"\s+", "", text)


def pick(row: dict[str, str], keys: tuple[str, ...]) -> str:
    normalized = {norm_key(k): v for k, v in row.items()}
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
        alt = normalized.get(norm_key(key))
        if alt is not None and str(alt).strip():
            return str(alt).strip()
    return ""


def clean_domain(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "://" in text:
        text = urlparse(text).netloc
    if "@" in text:
        text = text.rsplit("@", 1)[-1]
    text = text.split("/", 1)[0].split(":", 1)[0].strip(".")
    if text.startswith("www."):
        text = text[4:]
    return text if "." in text else ""


def domain_from_row(row: dict[str, str]) -> str:
    explicit = clean_domain(pick(row, DOMAIN_FIELDS))
    if explicit:
        return explicit
    for keys in (CONTACT_FIELDS, WEBSITE_FIELDS):
        domain = clean_domain(pick(row, keys))
        if domain:
            return domain
    return ""


def same_domain(left: str, right: str) -> bool:
    a = clean_domain(left)
    b = clean_domain(right)
    return bool(a and b and (a == b or a.endswith(f".{b}") or b.endswith(f".{a}")))


def parse_time(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=JST)
        except ValueError:
            continue
    return None


def latest_by_id(rows: list[dict[str, str]], id_fields: tuple[str, ...] = ID_FIELDS) -> dict[str, dict[str, str]]:
    latest: dict[str, dict[str, str]] = {}
    for row in rows:
        lead_id = pick(row, id_fields)
        if not lead_id:
            continue
        previous = latest.get(lead_id)
        if previous is None:
            latest[lead_id] = row
            continue
        row_time = parse_time(pick(row, ("timestamp",))) or datetime.min.replace(tzinfo=JST)
        prev_time = parse_time(pick(previous, ("timestamp",))) or datetime.min.replace(tzinfo=JST)
        if row_time >= prev_time:
            latest[lead_id] = row
    return latest


def latest_by_domain(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    latest: dict[str, dict[str, str]] = {}
    for row in rows:
        domain = domain_from_row(row)
        if not domain:
            continue
        previous = latest.get(domain)
        if previous is None:
            latest[domain] = row
            continue
        row_time = parse_time(pick(row, ("timestamp",))) or datetime.min.replace(tzinfo=JST)
        prev_time = parse_time(pick(previous, ("timestamp",))) or datetime.min.replace(tzinfo=JST)
        if row_time >= prev_time:
            latest[domain] = row
    return latest


def read_blocklist(path: Path) -> set[str]:
    domains: set[str] = set()
    if not path.exists():
        return domains
    for line in path.read_text(encoding="utf-8-sig", errors="replace").splitlines():
        value = line.split("#", 1)[0].strip()
        domain = clean_domain(value)
        if domain:
            domains.add(domain)
    return domains


def read_active_cooldowns(path: Path, now: datetime | None = None) -> set[str]:
    domains: set[str] = set()
    if not path.exists():
        return domains
    now = now or datetime.now(JST)
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return domains
    if not isinstance(data, dict):
        return domains
    for key, value in data.items():
        domain = clean_domain(str(key))
        if not domain:
            continue
        until_text = str(value.get("until", "")).strip() if isinstance(value, dict) else ""
        if not until_text:
            domains.add(domain)
            continue
        try:
            until = datetime.fromisoformat(until_text)
            if until.tzinfo is None:
                until = until.replace(tzinfo=JST)
        except ValueError:
            domains.add(domain)
            continue
        if until >= now:
            domains.add(domain)
    return domains


def contact_url_quality_issue(row: dict[str, str]) -> str:
    contact_url = pick(row, CONTACT_FIELDS)
    website = pick(row, WEBSITE_FIELDS)
    domain = domain_from_row(row)
    if not contact_url:
        return "weak_contact_url" if website else "unknown"
    parsed = urlparse(contact_url)
    fragment = (parsed.fragment or "").lower()
    path_text = " ".join([parsed.path or "", parsed.query or "", fragment]).lower()
    if fragment.startswith("toc"):
        return "weak_contact_url"
    if domain and parsed.hostname and not same_domain(parsed.hostname, domain):
        return "weak_contact_url"
    if not any(token.lower() in path_text for token in CONTACT_PATH_TOKENS):
        return "weak_contact_url"
    return "none"


def contains_any(text: str, tokens: tuple[str, ...]) -> bool:
    lower = str(text or "").lower()
    return any(token.lower() in lower for token in tokens)


def is_external_reservation_no_fill(status: str, reason: str, combined: str) -> bool:
    text = " ".join([status, reason, combined]).lower()
    return "no_form_fields" in text and contains_any(text, EXTERNAL_RESERVATION_TOKENS)


def classify_failure(
    *,
    status: str,
    reason: str,
    feedback: dict[str, str],
    source_row: dict[str, str],
    domain: str,
    blocklisted: bool,
    cooldown: bool,
    operational_context: str = "",
) -> str:
    combined = " ".join(
        [
            status,
            reason,
            operational_context,
            pick(feedback, ("lead_quality_issue",)),
            pick(feedback, ("contact_quality_issue",)),
            pick(feedback, ("form_quality_issue",)),
            pick(feedback, ("feedback_for_lead_finder",)),
            pick(feedback, ("lead_finder_exclusion_reason",)),
            pick(source_row, ("name_warning",)),
            pick(source_row, ("display_name", "business_name", "salon_name", "original__title")),
            pick(source_row, CONTACT_FIELDS),
        ]
    )
    if blocklisted or cooldown or "blocked_domain" in reason or "bot_protection" in combined:
        return "blocked_domain"
    if is_external_reservation_no_fill(status, reason, combined):
        return "external_reservation"
    if (
        "iframe_only_form" in combined
        or "embedded_or_external_form" in combined
        or "external_form" in combined
        or "manual_review_embedded_iframe_form" in combined
    ):
        return "external_form"
    if "timeout_contact" in combined:
        return "timeout_contact"
    if "no_form_fields" in combined:
        return "no_form_fields"
    if contains_any(combined, MEDIA_OR_LISTING_TOKENS):
        return "media_or_listing_page"
    if contains_any(combined, CORPORATE_OR_PORTAL_TOKENS):
        return "corporate_or_portal"
    if contact_url_quality_issue(source_row) == "weak_contact_url" or "weak_contact_url" in combined:
        return "weak_contact_url"
    name_confidence = str(pick(source_row, ("name_confidence",))).lower()
    name_warning = str(pick(source_row, ("name_warning",))).lower()
    if name_confidence in {"low", "unknown"} or any(
        token in name_warning for token in ("human_review", "manual_review", "low_confidence")
    ):
        return "low_confidence_name"
    return "none"


def recommendation_for(status: str, failure_category: str, feedback: dict[str, str]) -> str:
    explicit = pick(feedback, ("recommended_action", "lead_finder_recommended_action"))
    if explicit in {"block", "deprioritize", "manual_review", "retry_later", "improve_contact_url", "keep", "prioritize_similar"}:
        return explicit
    if failure_category == "blocked_domain":
        return "block"
    if failure_category == "timeout_contact":
        return "retry_later"
    if failure_category == "external_form":
        return "manual_review"
    if failure_category == "external_reservation":
        return "manual_review"
    if failure_category in {"no_form_fields", "weak_contact_url"}:
        return "improve_contact_url"
    if failure_category in {"media_or_listing_page", "corporate_or_portal", "low_confidence_name"}:
        return "deprioritize"
    if status == "prepared_full":
        return "prioritize_similar"
    if status.startswith("prepared_review") or status == "prepared_partial":
        return "manual_review"
    return "keep"


def score_adjustments(status: str, failure_category: str, feedback: dict[str, str]) -> tuple[str, str]:
    raw_penalty = pick(feedback, ("lead_selection_penalty", "lead_finder_score_penalty"))
    raw_bonus = pick(feedback, ("lead_selection_bonus",))
    penalty = int(abs(float(raw_penalty))) if raw_penalty not in {"", "none"} else 0
    bonus = int(float(raw_bonus)) if raw_bonus not in {"", "none"} else 0
    if status == "prepared_full":
        bonus = max(bonus, 20)
    if failure_category == "weak_contact_url":
        penalty = max(penalty, 25)
    elif failure_category == "external_reservation":
        penalty = max(penalty, 45)
    elif failure_category == "external_form":
        penalty = max(penalty, 40)
    elif failure_category == "low_confidence_name":
        penalty = max(penalty, 15)
    elif failure_category in {"no_form_fields", "timeout_contact"}:
        penalty = max(penalty, 60)
    elif failure_category in {"media_or_listing_page", "corporate_or_portal"}:
        penalty = max(penalty, 80)
    elif failure_category == "blocked_domain":
        penalty = max(penalty, 100)
    return str(bonus), str(penalty)


def outcome_for(status: str) -> str:
    """Classify automation workflow outcome without implying sales conversion."""
    if status == "prepared_full":
        return "workflow_success"
    return "unknown"


def load_feedback_rows(results_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(results_dir.glob("lead_quality_feedback_*.csv")):
        rows.extend(read_csv(path))
    return rows


def build_feedback(
    *,
    source_paths: list[Path],
    results_dir: Path,
    ledger_path: Path,
    blocklist_path: Path,
    cooldowns_path: Path,
    run_date: str,
    prior_feedback_paths: list[Path] | None = None,
) -> list[dict[str, str]]:
    source_rows: dict[tuple[str, str], dict[str, str]] = {}
    for source_path in source_paths:
        for index, row in enumerate(read_csv(source_path), 1):
            lead_id = pick(row, ID_FIELDS)
            domain = domain_from_row(row)
            key = (lead_id, domain)
            if key not in source_rows:
                next_row = dict(row)
                next_row.setdefault("source_row", str(index))
                next_row.setdefault("source_csv", str(source_path))
                source_rows[key] = next_row

    prior_feedback_rows: list[dict[str, str]] = []
    for prior_path in prior_feedback_paths or []:
        prior_feedback_rows.extend(read_csv(prior_path))
    for row in prior_feedback_rows:
        lead_id = pick(row, ("lead_id", "id", "salon_id"))
        domain = domain_from_row(row)
        if lead_id or domain:
            source_rows.setdefault((lead_id, domain), row)

    feedback_rows = load_feedback_rows(results_dir)
    feedback_rows.extend(prior_feedback_rows)
    feedback_by_id = latest_by_id(feedback_rows, ("lead_id", "id", "salon_id"))
    feedback_by_domain = latest_by_domain(feedback_rows)
    submission_rows: list[dict[str, str]] = []
    review_rows: list[dict[str, str]] = []
    for path in sorted(results_dir.glob("submissions_*.csv")):
        submission_rows.extend(read_csv(path))
    for path in sorted(results_dir.glob("review_queue_*.csv")):
        review_rows.extend(read_csv(path))
    submission_by_id = latest_by_id(submission_rows, ("salon_id", "lead_id", "id"))
    review_by_id = latest_by_id(review_rows, ("salon_id", "lead_id", "id"))
    ledger_by_id = latest_by_id(read_csv(ledger_path), ("salon_id", "lead_id", "id"))
    blocklist = read_blocklist(blocklist_path)
    cooldowns = read_active_cooldowns(cooldowns_path)

    for row in feedback_rows:
        lead_id = pick(row, ("lead_id", "id", "salon_id"))
        domain = domain_from_row(row)
        source_rows.setdefault((lead_id, domain), row)

    output: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for key, source in source_rows.items():
        lead_id, domain = key
        if key in seen:
            continue
        seen.add(key)
        feedback = feedback_by_id.get(lead_id) or feedback_by_domain.get(domain) or {}
        submission = submission_by_id.get(lead_id, {})
        review = review_by_id.get(lead_id, {})
        ledger = ledger_by_id.get(lead_id, {})
        status = (
            pick(submission, ("status",))
            or pick(review, ("status",))
            or pick(ledger, ("status",))
            or pick(feedback, ("final_status", "semi_auto_status"))
        )
        reason = (
            pick(submission, ("message", "reason"))
            or pick(review, ("reason",))
            or pick(ledger, ("reason",))
            or pick(feedback, ("reason", "semi_auto_reason"))
        )
        operational_context = " ".join(
            [
                pick(submission, ("evidence", "notes", "detected_platform", "last_action", "stop_state", "contact_url", "final_step_url", "url")),
                pick(review, ("evidence", "notes", "detected_platform", "last_action", "stop_state", "contact_url", "final_step_url")),
                pick(ledger, ("reason", "contact_url", "final_step_url")),
            ]
        )
        blocklisted = any(domain == item or domain.endswith(f".{item}") for item in blocklist)
        cooldown = any(domain == item or domain.endswith(f".{item}") for item in cooldowns)
        failure_category = classify_failure(
            status=status,
            reason=reason,
            feedback=feedback,
            source_row=source,
            domain=domain,
            blocklisted=blocklisted,
            cooldown=cooldown,
            operational_context=operational_context,
        )
        recommended_action = recommendation_for(status, failure_category, feedback)
        bonus, penalty = score_adjustments(status, failure_category, feedback)
        timestamp = pick(submission, ("timestamp",)) or pick(review, ("timestamp",)) or pick(ledger, ("timestamp",))
        row_date = (timestamp.split(" ", 1)[0] if timestamp else run_date)
        prepared_success = "1" if status == "prepared_full" else "0"
        manual_review = "1" if status in {"prepared_review_needed", "prepared_partial", "prepared_external"} else "0"
        blocked_or_skipped = "1" if status.startswith("skipped") or failure_category == "blocked_domain" else "0"
        output.append(
            {
                "lead_id": lead_id,
                "domain": domain,
                "display_name": pick(source, DISPLAY_FIELDS) or pick(feedback, ("display_name",)),
                "name_confidence": pick(source, ("name_confidence",)),
                "name_source": pick(source, ("name_source",)),
                "name_warning": pick(source, ("name_warning",)),
                "area": pick(source, AREA_FIELDS),
                "business_type": pick(source, TYPE_FIELDS),
                "category": pick(source, CATEGORY_FIELDS),
                "original_score": pick(source, SCORE_FIELDS),
                "solo_score": pick(source, SOLO_FIELDS),
                "contact_url": pick(feedback, ("contact_url",)) or pick(source, CONTACT_FIELDS),
                "website": pick(source, WEBSITE_FIELDS),
                "demo_url": pick(source, DEMO_FIELDS),
                "semi_auto_status": status,
                "semi_auto_reason": reason,
                "prepared_success": prepared_success,
                "manual_review_needed": manual_review,
                "blocked_or_skipped": blocked_or_skipped,
                "failure_category": failure_category,
                "recommended_action": recommended_action,
                "lead_selection_bonus": bonus,
                "lead_selection_penalty": penalty,
                "outcome": outcome_for(status),
                "outcome_source": "semi_auto_feedback" if status or reason else "unknown",
                "run_date": row_date,
            }
        )
    output.sort(key=lambda row: (row["domain"], row["lead_id"]))
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build local lead-quality feedback from SEMI_AUTO artifacts.")
    parser.add_argument("--source", action="append", default=[], help="Source handoff/lead CSV. May be repeated.")
    parser.add_argument("--results-dir", default=str(DEFAULT_RESULTS_DIR), help="Playwright results directory.")
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER), help="Playwright submission ledger CSV.")
    parser.add_argument("--blocklist", default=str(DEFAULT_BLOCKLIST), help="Playwright blocklist_domains.txt.")
    parser.add_argument("--cooldowns", default=str(DEFAULT_COOLDOWNS), help="Playwright domain_cooldowns.json.")
    parser.add_argument(
        "--prior-feedback",
        action="append",
        default=[],
        help="Existing local feedback CSV to preserve while refreshing latest artifacts. May be repeated.",
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output feedback CSV.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sources = [Path(item) for item in args.source] if args.source else [DEFAULT_SOURCE]
    run_date = datetime.now(JST).strftime("%Y-%m-%d")
    rows = build_feedback(
        source_paths=sources,
        results_dir=Path(args.results_dir),
        ledger_path=Path(args.ledger),
        blocklist_path=Path(args.blocklist),
        cooldowns_path=Path(args.cooldowns),
        run_date=run_date,
        prior_feedback_paths=[Path(item) for item in args.prior_feedback],
    )
    output_path = Path(args.output)
    write_csv(output_path, rows)
    print(f"output={output_path}")
    print(f"rows={len(rows)}")
    print(",".join(OUTPUT_FIELDS))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
