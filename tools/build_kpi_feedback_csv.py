#!/usr/bin/env python3
"""Build a local KPI feedback CSV from pipeline handoff and automation logs.

This adapter does not infer conversions or change scoring rules. It only joins
existing local status fields into a reviewable CSV that can later feed KPI,
benchmark, or bandit-style prioritization tools.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


JST = ZoneInfo("Asia/Tokyo")

OUTPUT_FIELDS = [
    "run_id",
    "date",
    "lead_id",
    "domain",
    "display_name",
    "area",
    "business_type",
    "category",
    "score",
    "name_confidence",
    "has_contact_url",
    "has_form",
    "demo_template",
    "message_template",
    "demo_url",
    "contact_url",
    "prepared_status",
    "sent_status",
    "skip_reason",
    "error_reason",
    "outcome",
    "outcome_source",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path or not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def pick(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def truthy(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "ok", "あり", "有", "○"}:
        return "1"
    if text in {"0", "false", "no", "n", "なし", "無", "×"}:
        return "0"
    return "1" if str(value or "").strip() else "0"


def is_web_url(value: str) -> bool:
    text = str(value or "").strip().lower()
    return text.startswith("http://") or text.startswith("https://")


def load_latest_by_id(path: Path, id_keys: tuple[str, ...]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in read_csv(path):
        row_id = pick(row, *id_keys)
        if row_id:
            out[row_id] = row
    return out


def normalize_outcome(status: str, reason: str) -> tuple[str, str, str, str]:
    status_l = str(status or "").strip().lower()
    reason_l = str(reason or "").strip().lower()
    if status_l == "sent":
        return "sent", "sent", "", ""
    if status_l.startswith("prepared") or status_l == "prepared":
        return "prepared", "", "", ""
    if status_l.startswith("skipped"):
        return "skipped", "", reason or status, ""
    if status_l == "failed" or "error" in status_l or reason_l.startswith("exception"):
        return "failed", "", "", reason or status
    return "unknown", "", "", ""


def build_rows(
    *,
    handoff_rows: list[dict[str, str]],
    generation_by_id: dict[str, dict[str, str]],
    ledger_by_id: dict[str, dict[str, str]],
    run_id: str,
    date: str,
    message_template: str,
) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for row in handoff_rows:
        lead_id = pick(row, "lead_id", "id", "ID")
        gen = generation_by_id.get(lead_id, {})
        ledger = ledger_by_id.get(lead_id, {})
        status = pick(ledger, "status") or pick(row, "status")
        reason = pick(ledger, "reason") or pick(row, "reason")
        outcome, sent_status, skip_reason, error_reason = normalize_outcome(status, reason)
        contact_url = pick(row, "contact_url", "contact_page", "original__contact_url", "original__form_url")
        output.append(
            {
                "run_id": run_id,
                "date": date,
                "lead_id": lead_id,
                "domain": pick(row, "domain", "original__domain"),
                "display_name": pick(row, "display_name", "business_name", "salon_name", "company_name", "brand_name", "店名"),
                "area": pick(row, "area", "location", "original__area_guess"),
                "business_type": pick(row, "business_type", "industry"),
                "category": pick(row, "category", "original__category_guess", "industry"),
                "score": pick(row, "score", "solo_score"),
                "name_confidence": pick(row, "name_confidence"),
                "has_contact_url": "1" if is_web_url(contact_url) else "0",
                "has_form": truthy(pick(row, "has_form", "original__has_form", "form_url", "original__form_url")),
                "demo_template": pick(gen, "template") or pick(row, "template"),
                "message_template": message_template,
                "demo_url": pick(row, "demo_url", "url(デモ)"),
                "contact_url": contact_url,
                "prepared_status": status if status.startswith("prepared") or status == "prepared" else "",
                "sent_status": sent_status,
                "skip_reason": skip_reason,
                "error_reason": error_reason,
                "outcome": outcome,
                "outcome_source": "ledger" if ledger else "handoff_status" if status or reason else "unknown",
            }
        )
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a KPI-ready feedback CSV from local pipeline outputs.")
    parser.add_argument("--handoff", required=True, help="handoff_with_demo_paths.csv or normalized handoff CSV")
    parser.add_argument("--generation-log", default="", help="Optional demo-generator output/generation_log.csv")
    parser.add_argument("--ledger", default="", help="Optional playwright-automation data/submission_ledger.csv")
    parser.add_argument("--output", required=True, help="Output KPI feedback CSV")
    parser.add_argument("--run-id", default="", help="Optional run id; defaults to timestamp")
    parser.add_argument("--message-template", default="config/message_template.txt", help="Message template identifier")
    args = parser.parse_args()

    now = datetime.now(JST)
    run_id = args.run_id or now.strftime("%Y%m%d_%H%M%S")
    date = now.strftime("%Y-%m-%d")
    handoff_path = Path(args.handoff)
    output_path = Path(args.output)
    handoff_rows = read_csv(handoff_path)
    generation_by_id = load_latest_by_id(Path(args.generation_log), ("id", "lead_id")) if args.generation_log else {}
    ledger_by_id = load_latest_by_id(Path(args.ledger), ("salon_id", "lead_id", "id")) if args.ledger else {}
    rows = build_rows(
        handoff_rows=handoff_rows,
        generation_by_id=generation_by_id,
        ledger_by_id=ledger_by_id,
        run_id=run_id,
        date=date,
        message_template=args.message_template,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"wrote {len(rows)} rows to {output_path}")
    print(",".join(OUTPUT_FIELDS))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
