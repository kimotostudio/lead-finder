#!/usr/bin/env python3
"""Local lead-supply quality policy for category-pivoted Fukuoka searches.

This module is intentionally pure local logic. It reads config/CSV inputs,
evaluates rows against upstream supply rules, and never searches the web or
opens a browser.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "search_terms_fukuoka_gate_passable_services.json"

LEAD_ID_FIELDS = ("lead_id", "id", "salon_id", "ID", "管理番号")
DOMAIN_FIELDS = ("domain", "original__domain")
WEBSITE_FIELDS = ("website", "url", "reference_url", "URL", "最終URL", "original__url", "url(旧)")
CONTACT_FIELDS = (
    "contact_url",
    "contact_page",
    "form_url",
    "inquiry_url",
    "original__contact_url",
    "original__form_url",
    "お問い合わせURL",
)
DISPLAY_FIELDS = (
    "display_name",
    "business_name",
    "salon_name",
    "brand_name",
    "company_name",
    "shop_name",
    "title",
    "店名",
)
LOCATION_FIELDS = ("location", "area", "area_guess", "address", "original__address", "original__area_guess")
TYPE_FIELDS = ("industry", "business_type", "category", "category_guess", "original__category_guess")
TEXT_FIELDS = (
    *DISPLAY_FIELDS,
    *LOCATION_FIELDS,
    *TYPE_FIELDS,
    "notes",
    "reason",
    "original_title",
    "original__title",
    "source_query",
)


@dataclass(frozen=True)
class SupplyEvaluation:
    lead_id: str
    domain: str
    display_name: str
    action: str
    reasons: tuple[str, ...]
    review_reasons: tuple[str, ...]
    source_preference: str
    source_score: int


def load_config(path: Path = DEFAULT_CONFIG) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _norm_key(value: str) -> str:
    return str(value or "").replace("\ufeff", "").strip().lower().replace("　", "").replace(" ", "")


def _pick(row: dict[str, str], keys: Iterable[str]) -> str:
    normalized = {_norm_key(key): value for key, value in row.items()}
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
        alt = normalized.get(_norm_key(key))
        if alt is not None and str(alt).strip():
            return str(alt).strip()
    return ""


def _clean_domain(value: str) -> str:
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


def _host(value: str) -> str:
    return (urlparse(str(value or "").strip()).hostname or "").lower().removeprefix("www.")


def _domain_for(row: dict[str, str]) -> str:
    explicit = _clean_domain(_pick(row, DOMAIN_FIELDS))
    if explicit:
        return explicit
    return _clean_domain(_pick(row, WEBSITE_FIELDS) or _pick(row, CONTACT_FIELDS))


def _domain_matches(domain: str, known_domains: Iterable[str]) -> bool:
    target = _clean_domain(domain)
    if not target:
        return False
    for item in known_domains:
        known = _clean_domain(str(item))
        if known and (target == known or target.endswith(f".{known}") or known.endswith(f".{target}")):
            return True
    return False


def _same_site(host_or_url: str, domain: str) -> bool:
    left = _clean_domain(host_or_url)
    right = _clean_domain(domain)
    return bool(left and right and (left == right or left.endswith(f".{right}") or right.endswith(f".{left}")))


def _contains_any(text: str, tokens: Iterable[str]) -> list[str]:
    haystack = str(text or "").lower()
    hits: list[str] = []
    for token in tokens:
        value = str(token or "").strip()
        if not value:
            continue
        token_l = value.lower()
        haystack_for_token = haystack.replace("東京都", "") if value == "京都" else haystack
        if value.isascii() and len(value) <= 3:
            matched = bool(re.search(rf"(?<![a-z0-9]){re.escape(token_l)}(?![a-z0-9])", haystack_for_token))
        else:
            matched = token_l in haystack_for_token
        if matched and value not in hits:
            hits.append(value)
    return hits


def _config_tokens(config: dict, *path: str) -> tuple[str, ...]:
    value: object = config
    for key in path:
        if not isinstance(value, dict):
            return ()
        value = value.get(key, ())
    if isinstance(value, list):
        return tuple(str(item) for item in value if str(item).strip())
    return ()


def _row_text(row: dict[str, str]) -> str:
    parts = [str(row.get(key, "") or "") for key in TEXT_FIELDS]
    parts.extend(str(value or "") for value in row.values())
    return " ".join(parts)


def _web_url(value: str) -> bool:
    parsed = urlparse(str(value or "").strip())
    return parsed.scheme.lower() in {"http", "https"} and bool(parsed.hostname)


def _has_deprioritized_tld(domain: str, tlds: Iterable[str]) -> bool:
    target = _clean_domain(domain)
    return any(target.endswith(str(tld or "").lower()) for tld in tlds if str(tld or "").strip())


def load_ledger_keys(path: Path) -> tuple[set[str], set[str]]:
    ids: set[str] = set()
    domains: set[str] = set()
    for row in read_csv(path):
        lead_id = _pick(row, ("salon_id", "lead_id", "id"))
        domain = _clean_domain(_pick(row, ("domain", "contact_url", "final_step_url", "url")))
        if lead_id:
            ids.add(lead_id)
        if domain:
            domains.add(domain)
    return ids, domains


def evaluate_row(
    row: dict[str, str],
    config: dict,
    *,
    ledger_ids: Iterable[str] = (),
    ledger_domains: Iterable[str] = (),
) -> SupplyEvaluation:
    lead_id = _pick(row, LEAD_ID_FIELDS)
    display_name = _pick(row, DISPLAY_FIELDS)
    website = _pick(row, WEBSITE_FIELDS)
    contact_url = _pick(row, CONTACT_FIELDS)
    domain = _domain_for(row)
    contact_host = _host(contact_url)
    row_text = _row_text(row)
    contact_text = " ".join([contact_url, website, row_text])

    required_geo = _config_tokens(config, "hard_geo", "row_required_tokens")
    out_of_area = _config_tokens(config, "hard_geo", "out_of_area_tokens")
    category_include = _config_tokens(config, "target_categories", "include")
    category_exclude = _config_tokens(config, "target_categories", "exclude")
    contact_prefer = _config_tokens(config, "contact_intent", "prefer")
    contact_avoid = _config_tokens(config, "contact_intent", "avoid")
    free_hosts = _config_tokens(config, "source_preference", "deprioritized_hosts")
    weak_tlds = _config_tokens(config, "source_preference", "deprioritized_tlds")
    aggregator_hosts = _config_tokens(config, "source_preference", "aggregator_hosts")
    external_contact_hosts = _config_tokens(config, "source_preference", "external_contact_hosts")

    hard_reasons: list[str] = []
    review_reasons: list[str] = []
    score = 0

    geo_hits = _contains_any(row_text, required_geo)
    if required_geo and not geo_hits:
        hard_reasons.append("missing_required_geo")
    else:
        score += 20

    out_hits = _contains_any(row_text, out_of_area)
    if out_hits:
        hard_reasons.append("out_of_area:" + ",".join(out_hits[:4]))

    include_hits = _contains_any(row_text, category_include)
    if include_hits:
        score += 20
    else:
        review_reasons.append("missing_target_category")

    excluded_category_hits = _contains_any(row_text, category_exclude)
    if excluded_category_hits:
        hard_reasons.append("excluded_category:" + ",".join(excluded_category_hits[:4]))

    plain_contact_hits = _contains_any(contact_text, contact_prefer)
    reservation_hits = _contains_any(contact_text, contact_avoid)
    if plain_contact_hits:
        score += 30
    else:
        review_reasons.append("missing_plain_contact_signal")
    if reservation_hits:
        hard_reasons.append("reservation_contact:" + ",".join(reservation_hits[:4]))

    ledger_id_set = {str(item) for item in ledger_ids if str(item).strip()}
    if lead_id and lead_id in ledger_id_set:
        hard_reasons.append("already_in_ledger:id")
    elif domain and _domain_matches(domain, ledger_domains):
        hard_reasons.append("already_in_ledger:domain")

    source_preference = "unknown_source"
    website_host = _host(website)
    is_free_host = _domain_matches(domain, free_hosts) or _domain_matches(website_host, free_hosts)
    is_weak_tld = _has_deprioritized_tld(domain, weak_tlds) or _has_deprioritized_tld(website_host, weak_tlds)
    is_aggregator = (
        _domain_matches(domain, aggregator_hosts)
        or _domain_matches(website_host, aggregator_hosts)
        or _domain_matches(contact_host, aggregator_hosts)
    )
    contact_is_known_external = _domain_matches(contact_host, external_contact_hosts)
    contact_is_external = bool(contact_url and contact_host and domain and not _same_site(contact_url, domain))
    own_domain_contact = bool(
        domain
        and _web_url(website or contact_url)
        and not is_free_host
        and not is_weak_tld
        and not is_aggregator
        and contact_url
        and _same_site(contact_url, domain)
    )

    if is_aggregator:
        source_preference = "aggregator_excluded"
        hard_reasons.append("source_aggregator")
        score -= 80
    elif contact_is_known_external or contact_is_external:
        source_preference = "external_contact_excluded"
        hard_reasons.append(f"external_contact_host:{contact_host or 'unknown'}")
        score -= 60
    elif own_domain_contact:
        source_preference = "own_domain_same_site_contact"
        score += 40
    elif is_free_host:
        source_preference = "free_host_deprioritized"
        review_reasons.append("free_host_deprioritized")
        score -= 25
    elif is_weak_tld:
        source_preference = "weak_tld_deprioritized"
        review_reasons.append("weak_tld_deprioritized")
        score -= 20

    hard_reasons = list(dict.fromkeys(hard_reasons))
    review_reasons = list(dict.fromkeys(review_reasons))
    action = "exclude" if hard_reasons else "review" if review_reasons else "allow"

    return SupplyEvaluation(
        lead_id=lead_id,
        domain=domain,
        display_name=display_name,
        action=action,
        reasons=tuple(hard_reasons),
        review_reasons=tuple(review_reasons),
        source_preference=source_preference,
        source_score=max(0, min(100, score)),
    )


def rank_rows(
    rows: Iterable[dict[str, str]],
    config: dict,
    *,
    ledger_ids: Iterable[str] = (),
    ledger_domains: Iterable[str] = (),
) -> list[SupplyEvaluation]:
    evaluations = [
        evaluate_row(row, config, ledger_ids=ledger_ids, ledger_domains=ledger_domains)
        for row in rows
    ]
    action_rank = {"allow": 0, "review": 1, "exclude": 2}
    return sorted(evaluations, key=lambda item: (action_rank[item.action], -item.source_score, item.lead_id, item.domain))


def audit_rows(evaluations: Iterable[SupplyEvaluation]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for item in evaluations:
        rows.append(
            {
                "lead_id": item.lead_id,
                "domain": item.domain,
                "display_name": item.display_name,
                "action": item.action,
                "source_preference": item.source_preference,
                "source_score": str(item.source_score),
                "reasons": ";".join(item.reasons),
                "review_reasons": ";".join(item.review_reasons),
            }
        )
    return rows


def write_audit(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "lead_id",
        "domain",
        "display_name",
        "action",
        "source_preference",
        "source_score",
        "reasons",
        "review_reasons",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def synthetic_fixture_rows() -> list[dict[str, str]]:
    return [
        {
            "lead_id": "own-contact",
            "display_name": "福岡設備サポート",
            "website": "https://fukuoka-setsubi.jp/",
            "contact_url": "https://fukuoka-setsubi.jp/contact/",
            "business_type": "福岡市 住宅設備 お問い合わせフォーム",
            "location": "福岡市",
        },
        {
            "lead_id": "reservation-salon",
            "display_name": "福岡プライベートサロン",
            "website": "https://salon.example.jp/",
            "contact_url": "https://salon.example.jp/reservation/",
            "business_type": "福岡市 サロン 予約フォーム",
            "location": "福岡市",
        },
        {
            "lead_id": "out-of-area",
            "display_name": "東京税理士オフィス",
            "website": "https://tokyo-tax.example.jp/",
            "contact_url": "https://tokyo-tax.example.jp/contact/",
            "business_type": "税理士 お問い合わせ",
            "location": "東京都",
        },
        {
            "lead_id": "ledger-row",
            "display_name": "福岡行政書士事務所",
            "website": "https://ledger-office.example.jp/",
            "contact_url": "https://ledger-office.example.jp/contact/",
            "business_type": "福岡市 行政書士 お問い合わせフォーム",
            "location": "福岡市",
        },
        {
            "lead_id": "free-host",
            "display_name": "福岡電気工事",
            "website": "https://fukuoka-denki.jimdofree.com/",
            "contact_url": "https://fukuoka-denki.jimdofree.com/contact/",
            "business_type": "福岡市 電気工事 お問い合わせフォーム",
            "location": "福岡市",
        },
        {
            "lead_id": "aggregator-row",
            "display_name": "福岡リフォーム",
            "website": "https://www.ekiten.jp/shop/example/",
            "contact_url": "https://www.ekiten.jp/shop/example/",
            "business_type": "福岡市 リフォーム お問い合わせ",
            "location": "福岡市",
        },
        {
            "lead_id": "external-form",
            "display_name": "福岡看板製作",
            "website": "https://fukuoka-sign.example.jp/",
            "contact_url": "https://ssl.form-mailer.jp/fms/example",
            "business_type": "福岡市 看板製作 お問い合わせフォーム",
            "location": "福岡市",
        },
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run local lead supply quality policy.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--input", default="", help="Optional local candidate CSV. Omit with --synthetic-fixture.")
    parser.add_argument("--ledger", default="", help="Optional local ledger CSV.")
    parser.add_argument("--audit-output", default="", help="Optional audit CSV output path.")
    parser.add_argument("--synthetic-fixture", action="store_true", help="Use built-in synthetic rows instead of CSV input.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config(Path(args.config))
    if args.synthetic_fixture or not args.input:
        rows = synthetic_fixture_rows()
        ledger_ids = {"ledger-row"}
        ledger_domains = {"ledger-office.example.jp"}
    else:
        rows = read_csv(Path(args.input))
        ledger_ids, ledger_domains = load_ledger_keys(Path(args.ledger)) if args.ledger else (set(), set())

    evaluations = rank_rows(rows, config, ledger_ids=ledger_ids, ledger_domains=ledger_domains)
    audits = audit_rows(evaluations)
    if args.audit_output:
        write_audit(Path(args.audit_output), audits)

    counts = Counter(item.action for item in evaluations)
    reason_counts = Counter(reason.split(":", 1)[0] for item in evaluations for reason in item.reasons)
    print(f"input_rows={len(rows)}")
    print(f"allow={counts.get('allow', 0)}")
    print(f"review={counts.get('review', 0)}")
    print(f"exclude={counts.get('exclude', 0)}")
    for key, value in sorted(reason_counts.items()):
        print(f"reason_{key}={value}")
    for item in evaluations:
        print(
            "row="
            + json.dumps(
                {
                    "lead_id": item.lead_id,
                    "action": item.action,
                    "source_preference": item.source_preference,
                    "source_score": item.source_score,
                    "reasons": item.reasons,
                    "review_reasons": item.review_reasons,
                },
                ensure_ascii=False,
            )
        )
    if args.audit_output:
        print(f"audit_output={Path(args.audit_output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
