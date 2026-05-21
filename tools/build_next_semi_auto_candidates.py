#!/usr/bin/env python3
"""Build a local next-run candidate CSV for SEMI_AUTO demo/outreach review.

This tool only reads and writes local CSV files. It does not search the web,
open browsers, generate messages, submit forms, or run Playwright.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

try:
    from tools.display_name_cleaner import clean_row_names
except ModuleNotFoundError:
    from display_name_cleaner import clean_row_names


JST = ZoneInfo("Asia/Tokyo")
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FEEDBACK = ROOT.parent / "playwright-automation" / "results" / "lead_quality_feedback_20260515.csv"
DEFAULT_KPI = ROOT.parent / "playwright-automation" / "results" / "semi_auto_kpi_20260515.csv"
DEFAULT_SUBMISSIONS = ROOT.parent / "playwright-automation" / "results" / "submissions_20260515.csv"
DEFAULT_REVIEW_QUEUE = ROOT.parent / "playwright-automation" / "results" / "review_queue_20260515.csv"
DEFAULT_LEDGER = ROOT.parent / "playwright-automation" / "data" / "submission_ledger.csv"
DEFAULT_LOCAL_FEEDBACK = ROOT / "ops_runs" / "lead_quality_feedback_latest.csv"
DEFAULT_BLOCKLIST = ROOT.parent / "playwright-automation" / "data" / "blocklist_domains.txt"
DEFAULT_COOLDOWNS = ROOT.parent / "playwright-automation" / "data" / "domain_cooldowns.json"

BASE_FIELDS = [
    "lead_id",
    "id",
    "company_name",
    "business_name",
    "display_name",
    "salon_name",
    "brand_name",
    "name_confidence",
    "name_source",
    "name_warning",
    "original_display_name",
    "original_title",
    "website",
    "url",
    "reference_url",
    "contact_page",
    "contact_url",
    "industry",
    "business_type",
    "location",
    "area",
    "score",
    "solo_score",
    "notes",
    "domain",
    "demo_path",
    "demo_url",
    "message_path",
    "message",
    "status",
    "reason",
    "source_csv",
    "source_row",
    "template",
    "image",
    "therapist_image",
    "url(旧)",
    "url(デモ)",
    "店名",
]

QUALITY_FIELDS = [
    "lead_tier",
    "quality_score",
    "exclusion_reasons",
    "review_reasons",
    "contact_quality",
    "name_quality",
    "recommended_action",
    "tier_b_promotion_decision",
    "tier_b_promotion_reason",
    "tier_b_promotion_action",
    "lead_selection_bonus",
    "lead_selection_penalty",
    "semi_auto_quality_score",
    "semi_auto_quality_issue",
    "semi_auto_exclusion_reason",
    "semi_auto_contact_quality",
    "semi_auto_location_match",
    "semi_auto_feedback_action",
    "semi_auto_feedback_penalty",
    "semi_auto_feedback_bonus",
    "failure_category",
]

LEAD_ID_FIELDS = ("lead_id", "id", "ID", "管理番号")
NAME_FIELDS = (
    "display_name",
    "表示名",
    "business_name",
    "salon_name",
    "brand_name",
    "store_name",
    "shop_name",
    "company_name",
    "店舗名",
    "店名",
    "name",
    "title",
    "original__title",
)
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
DOMAIN_FIELDS = ("domain", "original__domain")
SCORE_FIELDS = ("lead_score", "リードスコア", "score", "スコア", "solo_score", "original__solo_score")
TYPE_FIELDS = ("business_type", "industry", "業種", "category", "category_guess", "original__category_guess")
AREA_FIELDS = ("area", "area_guess", "original__area_guess", "location", "市区町村", "地方", "address", "original__address")
NOTES_FIELDS = ("notes", "reason", "original__reason", "営業ラベル理由", "コメント", "フィルタ理由", "個人度理由")

NAME_CONFIDENCE_RANK = {"low": 0, "unknown": 0, "": 0, "medium": 1, "high": 2}
LOW_NAME_VALUES = {"low", "unknown", "uncertain", "domain_fallback"}
TITLE_DERIVED_SOURCE_VALUES = {"title", "site_title", "original__title", "original__original__title", "title_cleaned"}
DOMAIN_DERIVED_SOURCE_VALUES = {"domain", "domain_fallback"}
PRIVATE_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
IGNORED_SCHEMES = {"tel", "mailto", "line", "sms", "javascript", "data"}

CORPORATE_TOKENS = [
    "corporate",
    "corporation",
    "inc.",
    " inc",
    "ltd.",
    "company",
    "株式会社",
    "有限会社",
    "合同会社",
    "法人",
    "クリニック",
    "医療脱毛",
    "メンズ医療脱毛",
    "世界最大級",
    "フランチャイズ",
    "全国展開",
    "多店舗",
    "グループ",
]
CORPORATE_CHAIN_HOSTS = [
    "kobekyo.com",
    "nova.co.jp",
    "pilates-k.jp",
    "m-pilates.com",
]
MEDICAL_TOKENS = [
    "医療",
    "医療法人",
    "病院",
    "医院",
    "診療",
    "クリニック",
    "美容皮膚科",
    "皮膚科",
    "歯科",
    "内科",
    "外科",
    "小児科",
    "婦人科",
    "泌尿器",
    "看護",
    "医療脱毛",
]
LINE_SNS_HOSTS = [
    "lin.ee",
    "line.me",
    "page.line.me",
    "instagram.com",
    "facebook.com",
    "x.com",
    "twitter.com",
    "tiktok.com",
    "linktr.ee",
    "lit.link",
]
PORTAL_HOSTS = [
    "beauty.hotpepper.jp",
    "hotpepper.jp",
    "hotpepperbeauty.jp",
    "candfans.jp",
    "findglocal.com",
    "goo.ne.jp",
    "minimodel.jp",
    "epark.jp",
    "ekiten.jp",
    "job-medley.com",
    "maps.google.jp",
    "rakuten.co.jp",
    "rakuten.ne.jp",
    "tabelog.com",
    "gnavi.co.jp",
    "gurunavi.com",
    "mybest.com",
    "mapion.co.jp",
    "google.com",
    "google.co.jp",
    "instabase.jp",
    "honkaku-uranai.jp",
    "music.kawai.jp",
    "note.com",
    "select-type.com",
    "tayori.com",
    "city.fukuoka.lg.jp",
    "lg.jp",
]
EXTERNAL_BOOKING_HOSTS = [
    "appt.salondenet.jp",
    "salondenet.jp",
    "reserva.be",
    "airrsv.net",
    "coubic.com",
    "tol-app.jp",
    "reservestock.jp",
]
AUTOMATION_HARD_FAILURE_CATEGORIES = {
    "blocked_domain",
    "no_form_fields",
    "timeout_contact",
    "media_or_listing_page",
    "corporate_or_portal",
    "robots_disallow",
    "robot_disallow",
    "robots_txt_disallow",
}
TIER_B_MANUAL_REVIEW_REASONS = {
    "feedback_external_form",
    "weak_contact",
    "toc_anchor_contact",
    "external_contact",
    "website_fallback_contact",
    "contact_unknown",
    "medium_name_confidence",
    "low_name_confidence",
    "below_min_quality_score",
    "location_mismatch",
    "feedback_deprioritize",
}
TIER_B_PROMOTABLE_REVIEW_REASONS = {"medium_name_confidence"}
PORTAL_TOKENS = [
    "ポータル",
    "掲載",
    "広告",
    "媒体",
    "ホットペッパー",
    "portal",
    "listing",
    "directory",
    "media",
    "advertis",
    "hotpepper",
    "findglocal",
    "minimodel",
    "探している人",
    "発見するサイト",
    "大学",
    "university",
]
UNSUITABLE_TOKENS = [
    "アダルト",
    "風俗",
    "セックス",
    "エッチしたい",
    "adult",
    "sex ",
    "dating",
]
CONTACT_PATH_TOKENS = [
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
]
WEAK_ANCHOR_RE = re.compile(r"^#?(?:toc|目次|heading|section)[-_]?\d*$", re.IGNORECASE)
NOISY_NAME_TOKENS = [
    "google",
    "口コミ",
    "レビュー",
    "ランキング",
    "検索",
    "地図",
    "住所",
    "営業時間",
    "公式サイト",
    "ホームページ",
]
CATEGORY_ONLY_NAMES = {"整体", "エステ", "脱毛", "美容室", "美容院", "サロン", "鍼灸", "ネイル", "home", "top"}


@dataclass
class CandidateEval:
    row: dict[str, str]
    audit_row: dict[str, str]
    selected: bool
    quality_score: int
    exclusion_reasons: list[str]
    review_reasons: list[str]
    hard_exclusion_reasons: list[str]
    lead_tier: str


def _dedupe(items: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(item for item in items if item))


def _reason_key(reason: str) -> str:
    return str(reason or "").split(":", 1)[0]


def _norm_key(value: str) -> str:
    text = str(value or "").replace("\ufeff", "").strip().lower()
    text = text.replace("（", "(").replace("）", ")").replace("　", " ")
    return re.sub(r"\s+", "", text)


def _pick(row: dict[str, str], keys: Iterable[str]) -> str:
    normalized = {_norm_key(k): v for k, v in row.items()}
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
        alt = normalized.get(_norm_key(key))
        if alt is not None and str(alt).strip():
            return str(alt).strip()
    return ""


def _clean_domain(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if "://" in raw:
        raw = urlparse(raw).netloc
    if "@" in raw:
        raw = raw.rsplit("@", 1)[-1]
    raw = raw.split("/", 1)[0].split(":", 1)[0].strip(".")
    if raw.startswith("www."):
        raw = raw[4:]
    return raw if "." in raw else ""


def _extract_domain(*values: str) -> str:
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue
        scheme = re.match(r"^([a-z][a-z0-9+.-]*):", raw, re.IGNORECASE)
        if scheme and scheme.group(1).lower() in IGNORED_SCHEMES:
            continue
        domain = _clean_domain(raw)
        if domain:
            return domain
    return ""


def _slug(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^\w]+", "-", text, flags=re.UNICODE)
    text = re.sub(r"-+", "-", text).strip("-_")
    return text[:56].strip("-_")


def _generated_lead_id(domain: str, display_name: str, source_row: int) -> str:
    domain_slug = _slug(domain.replace(".", "-"))
    if domain_slug:
        return f"lf-{domain_slug}"
    name_slug = _slug(display_name)
    return f"lf-{name_slug or 'unknown'}-r{source_row:05d}"


def _is_web_url(value: str) -> bool:
    parsed = urlparse(str(value or "").strip())
    host = (parsed.hostname or "").lower()
    if parsed.scheme.lower() not in {"http", "https"} or not host:
        return False
    if host in PRIVATE_HOSTS or host.endswith(".local"):
        return False
    return True


def _host(value: str) -> str:
    parsed = urlparse(str(value or "").strip())
    return (parsed.hostname or "").lower()


def _same_site(host_or_url: str, domain: str) -> bool:
    left = _clean_domain(host_or_url)
    right = _clean_domain(domain)
    return bool(left and right and (left == right or left.endswith(f".{right}") or right.endswith(f".{left}")))


def _host_matches(host_or_domain: str, known_hosts: Iterable[str]) -> bool:
    domain = _clean_domain(host_or_domain)
    return any(domain == host or domain.endswith(f".{host}") for host in known_hosts)


def _domain_matches(domain: str, known_domains: Iterable[str]) -> bool:
    target = _clean_domain(domain)
    return any(target == item or target.endswith(f".{item}") for item in known_domains if item)


def _contains_any(text: str, tokens: Iterable[str]) -> bool:
    haystack = str(text or "").lower()
    return any(token.lower() in haystack for token in tokens)


def _truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "ok", "あり", "有", "○"}


def _to_float(value: str) -> float:
    try:
        return float(str(value or "").replace(",", "").strip())
    except ValueError:
        return 0.0


def _latest_playwright_csv(pattern: str, fallback: Path) -> Path:
    candidates = sorted((ROOT.parent / "playwright-automation" / "results").glob(pattern), reverse=True)
    return candidates[0] if candidates else fallback


def _contact_url_has_good_path(value: str) -> bool:
    parsed = urlparse(str(value or "").strip())
    text = " ".join([parsed.path or "", parsed.query or "", parsed.fragment or ""]).lower()
    return any(token.lower() in text for token in CONTACT_PATH_TOKENS)


def _has_toc_like_anchor(value: str) -> bool:
    parsed = urlparse(str(value or "").strip())
    fragment = (parsed.fragment or "").strip()
    return bool(fragment and WEAK_ANCHOR_RE.match(fragment))


def _tier_b_promotion_policy(
    *,
    lead_tier: str,
    quality_score: int,
    contact_quality: str,
    contact_url: str,
    contact_host: str,
    direct_contact: bool,
    good_contact_path: bool,
    toc_anchor: bool,
    has_form: bool,
    review_reasons: list[str],
    hard_exclusion_reasons: list[str],
    failure_category: str,
) -> tuple[str, str, str]:
    """Classify Tier B rows for manual promotion to demo generation.

    Tier B is intentionally conservative: a row can be reviewable without being
    ready for the automated demo -> SEMI_AUTO path.
    """
    hard_keys = {_reason_key(reason) for reason in hard_exclusion_reasons}
    review_keys = {_reason_key(reason) for reason in review_reasons}
    failure_key = _reason_key(failure_category)

    if lead_tier == "A":
        return "not_applicable_tier_a", "Tier A rows already use the normal demo-ready path.", "use_tier_a"
    if lead_tier == "C" or hard_keys:
        reason = ";".join(hard_exclusion_reasons) or "tier_c"
        return "exclude", f"Hard exclusion present: {reason}.", "exclude_from_automated_path"
    if failure_key in AUTOMATION_HARD_FAILURE_CATEGORIES:
        return (
            "exclude",
            f"Prior SEMI_AUTO feedback indicates automated path failure: {failure_key}.",
            "exclude_from_automated_path",
        )
    if _host_matches(contact_host, EXTERNAL_BOOKING_HOSTS):
        return (
            "exclude",
            f"Contact URL uses external booking platform host {contact_host}; keep out of automated path.",
            "exclude_from_automated_path",
        )
    if contact_quality == "external":
        return (
            "keep_for_manual_review",
            "External contact URL needs human judgment before promotion.",
            "manual_check_contact_path_before_demo",
        )
    if contact_quality in {"non_web", "unknown"} or not direct_contact:
        return (
            "keep_for_manual_review",
            "No direct same-site contact path suitable for SEMI_AUTO promotion.",
            "manual_check_contact_path_before_demo",
        )
    if toc_anchor or not good_contact_path:
        return (
            "keep_for_manual_review",
            "Contact URL is an anchor or weak path rather than a confirmed form path.",
            "manual_check_contact_path_before_demo",
        )
    manual_reasons = review_keys & TIER_B_MANUAL_REVIEW_REASONS
    if manual_reasons - TIER_B_PROMOTABLE_REVIEW_REASONS:
        return (
            "keep_for_manual_review",
            f"Review reasons need human judgment: {';'.join(sorted(manual_reasons))}.",
            "manual_review_before_demo",
        )
    if not has_form:
        return (
            "keep_for_manual_review",
            "No local form signal; promote only after human contact-path confirmation.",
            "manual_check_contact_path_before_demo",
        )
    if quality_score < 75:
        return (
            "keep_for_manual_review",
            f"Quality score {quality_score} is below Tier B promotion threshold.",
            "manual_review_before_demo",
        )
    return (
        "promote_to_demo",
        "Direct same-site form path, usable score, and only benign Tier B review reasons.",
        "send_to_demo_generator_then_playwright_preflight_only",
    )


def _confidence_rank(value: str) -> int:
    return NAME_CONFIDENCE_RANK.get(str(value or "").strip().lower(), 0)


def _infer_name_confidence(display_name: str, source: str) -> tuple[str, str]:
    text = str(display_name or "").strip()
    compact = re.sub(r"\s+", "", text)
    warnings: list[str] = []
    if not text:
        return "low", "missing"
    if len(text) > 36:
        warnings.append("long_name")
    if "." in compact and re.search(r"[a-z0-9-]+\.[a-z]", compact.lower()):
        warnings.append("domain_like")
    if compact.lower() in CATEGORY_ONLY_NAMES:
        warnings.append("category_only")
    if _contains_any(text, NOISY_NAME_TOKENS):
        warnings.append("title_or_location_noise")
    if source.lower() in {"title", "original__title"}:
        warnings.append("title_source")

    if any(item in warnings for item in ("missing", "domain_like", "category_only", "title_or_location_noise")):
        return "low", ";".join(warnings)
    if warnings:
        return "medium", ";".join(warnings)
    return "high", ""


def _adjust_name_confidence_for_domain(confidence: str, warning: str, display_name: str, domain: str) -> tuple[str, str]:
    text = str(display_name or "").strip()
    compact = re.sub(r"\s+", "", text).lower()
    has_word_spacing = bool(re.search(r"\s", text))
    domain_label = _clean_domain(domain).split(".", 1)[0]
    warnings = [item for item in str(warning or "").split(";") if item]
    if compact and domain_label and re.fullmatch(r"[a-z0-9-]{3,24}", compact):
        if compact == domain_label or (not has_word_spacing and (domain_label.startswith(compact) or compact.startswith(domain_label))):
            warnings.append("domain_label_name")
            confidence = "low"
        elif "-" in compact or re.search(r"\d", compact):
            warnings.append("slug_like_ascii_name")
            confidence = "low"
        elif compact == text and len(compact) <= 14:
            warnings.append("short_ascii_name")
            if _confidence_rank(confidence) > _confidence_rank("medium"):
                confidence = "medium"
    return confidence, ";".join(dict.fromkeys(warnings))


def _combined_text(row: dict[str, str]) -> str:
    keys = [
        *NAME_FIELDS,
        *TYPE_FIELDS,
        *NOTES_FIELDS,
        "original_display_name",
        "original_title",
        "original__title",
        "original__category_guess",
        "個人度分類",
        "法人語検出",
        "AIフィルタ理由",
        "AIフィルタフラグ",
    ]
    return " ".join(str(row.get(key, "")) for key in keys)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _load_feedback(path: Path) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    by_id: dict[str, dict[str, str]] = {}
    by_domain: dict[str, dict[str, str]] = {}
    for row in _read_csv(path):
        lead_id = _pick(row, ("lead_id", "id", "salon_id"))
        domain = _clean_domain(_pick(row, ("domain", "contact_url", "final_step_url")))
        if lead_id:
            by_id[lead_id] = row
        if domain:
            by_domain[domain] = row
    return by_id, by_domain


def _merge_feedback_maps(paths: Iterable[Path]) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    by_id: dict[str, dict[str, str]] = {}
    by_domain: dict[str, dict[str, str]] = {}
    for path in paths:
        next_by_id, next_by_domain = _load_feedback(path)
        for lead_id, row in next_by_id.items():
            merged = dict(by_id.get(lead_id, {}))
            merged.update({key: value for key, value in row.items() if str(value or "").strip()})
            by_id[lead_id] = merged
        for domain, row in next_by_domain.items():
            merged = dict(by_domain.get(domain, {}))
            merged.update({key: value for key, value in row.items() if str(value or "").strip()})
            by_domain[domain] = merged
    return by_id, by_domain


def _load_blocklist_domains(path: Path) -> set[str]:
    domains: set[str] = set()
    if not path.exists():
        return domains
    for line in path.read_text(encoding="utf-8-sig", errors="replace").splitlines():
        value = line.split("#", 1)[0].strip()
        domain = _clean_domain(value)
        if domain:
            domains.add(domain)
    return domains


def _load_active_cooldown_domains(path: Path, now: datetime | None = None) -> set[str]:
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
        domain = _clean_domain(str(key))
        if not domain:
            continue
        until_text = ""
        if isinstance(value, dict):
            until_text = str(value.get("until", "")).strip()
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


def _load_ledger_keys(path: Path) -> tuple[set[str], set[str]]:
    ids: set[str] = set()
    domains: set[str] = set()
    for row in _read_csv(path):
        lead_id = _pick(row, ("salon_id", "lead_id", "id"))
        domain = _clean_domain(_pick(row, ("domain", "contact_url", "final_step_url")))
        if lead_id:
            ids.add(lead_id)
        if domain:
            domains.add(domain)
    return ids, domains


def _load_touched_keys(paths: Iterable[Path]) -> tuple[set[str], set[str]]:
    ids: set[str] = set()
    domains: set[str] = set()
    for path in paths:
        for row in _read_csv(path):
            lead_id = _pick(row, ("salon_id", "lead_id", "id"))
            domain = _clean_domain(_pick(row, ("domain", "contact_url", "final_step_url", "url")))
            if lead_id:
                ids.add(lead_id)
            if domain:
                domains.add(domain)
    return ids, domains


def _normalize_row(row: dict[str, str], source_csv: Path, source_row: int) -> dict[str, str]:
    website = _pick(row, WEBSITE_FIELDS)
    explicit_contact = _pick(row, CONTACT_FIELDS)
    contact_url = explicit_contact or website
    domain = _extract_domain(_pick(row, DOMAIN_FIELDS), website, contact_url)
    _name_row, name_result = clean_row_names(row, domain)
    display_name = name_result.display_name
    confidence = name_result.name_confidence
    warning = name_result.name_warning
    name_source = name_result.name_source
    provided_confidence = _pick(row, ("name_confidence",))
    provided_source = _pick(row, ("name_source",)).lower()
    if provided_confidence and (
        name_result.name_source == "explicit"
        or provided_source in DOMAIN_DERIVED_SOURCE_VALUES
        or (provided_confidence.lower() == "low" and provided_source not in TITLE_DERIVED_SOURCE_VALUES)
    ):
        confidence = provided_confidence
        warning = _pick(row, ("name_warning",)) or warning
        if provided_source in DOMAIN_DERIVED_SOURCE_VALUES:
            name_source = "domain"
    business_name = display_name or domain
    lead_id = _pick(row, LEAD_ID_FIELDS) or _generated_lead_id(domain, business_name, source_row)
    if name_source != "domain":
        confidence, warning = _adjust_name_confidence_for_domain(confidence, warning, display_name, domain)
    score = _pick(row, SCORE_FIELDS)
    notes = _pick(row, NOTES_FIELDS)
    normalized = {
        "lead_id": lead_id,
        "id": lead_id,
        "company_name": business_name,
        "business_name": business_name,
        "display_name": display_name or domain,
        "salon_name": business_name,
        "brand_name": business_name,
        "name_confidence": confidence,
        "name_source": name_source,
        "name_warning": warning,
        "original_display_name": name_result.original_display_name,
        "original_title": name_result.original_title,
        "website": website,
        "url": website,
        "reference_url": website,
        "contact_page": explicit_contact,
        "contact_url": contact_url,
        "industry": _pick(row, ("industry", "業種")) or _pick(row, TYPE_FIELDS),
        "business_type": _pick(row, TYPE_FIELDS),
        "location": _pick(row, ("location", "市区町村", "地方")) or _pick(row, AREA_FIELDS),
        "area": _pick(row, AREA_FIELDS),
        "score": score,
        "solo_score": _pick(row, ("solo_score", "個人度スコア(raw)", "個人度スコア(0-100)")) or score,
        "notes": notes,
        "domain": domain,
        "demo_path": _pick(row, ("demo_path",)),
        "demo_url": _pick(row, ("demo_url", "url(デモ)")),
        "message_path": _pick(row, ("message_path",)),
        "message": _pick(row, ("message",)),
        "status": _pick(row, ("status",)),
        "reason": _pick(row, ("reason",)),
        "source_csv": str(source_csv),
        "source_row": str(source_row),
        "template": _pick(row, ("template",)),
        "image": _pick(row, ("image",)),
        "therapist_image": _pick(row, ("therapist_image",)),
        "url(旧)": website,
        "url(デモ)": _pick(row, ("url(デモ)", "demo_url")),
        "店名": business_name,
    }
    for key, value in row.items():
        clean_key = str(key or "").replace("\ufeff", "").strip()
        if not clean_key:
            continue
        normalized.setdefault(f"original__{clean_key}", str(value or "").strip())
    return normalized


def _evaluate_row(
    row: dict[str, str],
    *,
    feedback_by_id: dict[str, dict[str, str]],
    feedback_by_domain: dict[str, dict[str, str]],
    ledger_ids: set[str],
    ledger_domains: set[str],
    blocklist_domains: set[str],
    cooldown_domains: set[str],
    include_ledger_domains: bool,
    include_blocklisted: bool,
    include_cooldown_domains: bool,
    allow_corporate: bool,
    allow_line_sns: bool,
    allow_portal_listing: bool,
    allow_weak_contact: bool,
    min_name_confidence: str,
    min_quality_score: int,
    required_location_tokens: tuple[str, ...],
) -> CandidateEval:
    lead_id = row["lead_id"]
    domain = row["domain"]
    website = row["website"]
    contact_url = row["contact_url"]
    explicit_contact = row["contact_page"]
    contact_host = _host(contact_url)
    text = _combined_text(row)
    location_text = " ".join(
        str(row.get(key, ""))
        for key in (
            "display_name",
            "business_name",
            "website",
            "url",
            "contact_url",
            "location",
            "area",
            "original__title",
            "original__address",
            "original__area_guess",
            "original__city",
        )
    )
    location_match = not required_location_tokens or any(token in location_text for token in required_location_tokens)
    feedback = feedback_by_id.get(lead_id) or feedback_by_domain.get(domain) or {}
    feedback_action = _pick(feedback, ("lead_finder_recommended_action", "recommended_action"))
    raw_feedback_penalty = _to_float(_pick(feedback, ("lead_finder_score_penalty", "lead_selection_penalty")))
    feedback_penalty = int(abs(raw_feedback_penalty))
    feedback_bonus = int(_to_float(_pick(feedback, ("lead_selection_bonus",))))
    feedback_exclusion = _pick(feedback, ("lead_finder_exclusion_reason", "failure_category"))
    failure_category = _pick(feedback, ("failure_category",))
    semi_auto_status = _pick(feedback, ("final_status", "semi_auto_status"))
    name_rank = _confidence_rank(row["name_confidence"])
    min_name_rank = _confidence_rank(min_name_confidence)
    has_form = _truthy(_pick(row, ("has_form", "original__has_form", "original__original__has_form", "生存")))
    has_contact_signal = bool(explicit_contact) or _truthy(
        _pick(row, ("has_contact_page", "original__has_contact_page", "original__original__has_contact_page"))
    )
    direct_contact = bool(explicit_contact and contact_url and _same_site(contact_url, domain))
    non_web_contact = bool(explicit_contact and not _is_web_url(explicit_contact))
    good_contact_path = _contact_url_has_good_path(contact_url)
    toc_anchor = _has_toc_like_anchor(contact_url)
    website_ok = _is_web_url(website or contact_url)
    corporate_like = (
        _contains_any(text, CORPORATE_TOKENS)
        or _host_matches(domain, CORPORATE_CHAIN_HOSTS)
        or _host_matches(_host(website), CORPORATE_CHAIN_HOSTS)
        or _host_matches(contact_host, CORPORATE_CHAIN_HOSTS)
        or "corporate" in _pick(row, ("個人度分類", "original__個人度分類")).lower()
        or _truthy(_pick(row, ("法人語検出", "original__法人語検出")))
    )
    medical_like = _contains_any(text, MEDICAL_TOKENS)
    line_sns = _host_matches(domain, LINE_SNS_HOSTS) or _host_matches(contact_host, LINE_SNS_HOSTS)
    portal_listing = _host_matches(domain, PORTAL_HOSTS) or _host_matches(contact_host, PORTAL_HOSTS) or _contains_any(text, PORTAL_TOKENS)
    unsuitable_content = _contains_any(text, UNSUITABLE_TOKENS)
    external_contact = bool(explicit_contact and contact_host and domain and not _same_site(contact_host, domain))
    weak_contact = (not website_ok) or line_sns or portal_listing or external_contact
    if toc_anchor or not explicit_contact:
        weak_contact = True
    if non_web_contact:
        weak_contact = True
    if explicit_contact and direct_contact and good_contact_path and not toc_anchor:
        weak_contact = False
    if has_contact_signal is False and _pick(
        row,
        (
            "original__has_contact_page",
            "original__original__has_contact_page",
            "has_contact_page",
            "original__has_form",
            "original__original__has_form",
            "has_form",
        ),
    ):
        weak_contact = True
    blocklisted = _domain_matches(domain, blocklist_domains)
    cooldown = _domain_matches(domain, cooldown_domains)
    duplicate_ledger = not include_ledger_domains and (lead_id in ledger_ids or _host_matches(domain, ledger_domains))
    feedback_hard_exclude = (
        feedback_action in {"exclude", "block"}
        or raw_feedback_penalty <= -100
        or _reason_key(failure_category) in AUTOMATION_HARD_FAILURE_CATEGORIES
    )

    issues: list[str] = []
    if blocklisted:
        issues.append("blocklist_domain")
    if cooldown:
        issues.append("cooldown_domain")
    if corporate_like:
        issues.append("corporate_like")
    if medical_like:
        issues.append("medical_like")
    if line_sns:
        issues.append("line_or_sns")
    if portal_listing:
        issues.append("portal_listing")
    if unsuitable_content:
        issues.append("unsuitable_content")
    if weak_contact:
        issues.append("weak_contact")
    if _reason_key(failure_category) in {"external_form", "iframe_only_form"}:
        issues.append("external_form")
    if not website_ok:
        issues.append("no_usable_website")
    if non_web_contact:
        issues.append("non_web_contact")
    if toc_anchor:
        issues.append("toc_anchor_contact")
    if name_rank < min_name_rank or row["name_confidence"].lower() in LOW_NAME_VALUES:
        issues.append("low_name_confidence")
    if not has_contact_signal and not has_form:
        issues.append("contact_unknown")
    if not location_match:
        issues.append("location_mismatch")

    original_score = _to_float(_pick(row, SCORE_FIELDS) or row.get("score", ""))
    if original_score > 0 and original_score <= 10:
        original_score *= 10
    quality_score = int(max(0, min(100, original_score)))
    if name_rank >= 2:
        quality_score += 10
    elif name_rank <= 0:
        quality_score -= 20
    if direct_contact:
        quality_score += 10
    if has_form:
        quality_score += 5
    if good_contact_path and direct_contact:
        quality_score += 8
    if not has_contact_signal:
        quality_score -= 30
    if corporate_like:
        quality_score -= 45
    if medical_like:
        quality_score -= 65
    if line_sns:
        quality_score -= 40
    if portal_listing:
        quality_score -= 35
    if unsuitable_content:
        quality_score -= 100
    if weak_contact:
        quality_score -= 25
    if blocklisted:
        quality_score -= 100
    if cooldown:
        quality_score -= 80
    quality_score += feedback_bonus
    quality_score -= feedback_penalty
    quality_score = int(max(0, min(100, quality_score)))

    exclusion_reasons: list[str] = []
    if blocklisted and not include_blocklisted:
        exclusion_reasons.append("blocklist_domain")
    if cooldown and not include_cooldown_domains:
        exclusion_reasons.append("cooldown_domain")
    if duplicate_ledger:
        exclusion_reasons.append("duplicate_ledger")
    if feedback_hard_exclude or feedback_action == "deprioritize":
        exclusion_reasons.append(f"feedback_exclude:{feedback_exclusion or 'prior_outcome'}")
    if corporate_like and not allow_corporate:
        exclusion_reasons.append("corporate_like")
    if medical_like:
        exclusion_reasons.append("medical_like")
    if line_sns and not allow_line_sns:
        exclusion_reasons.append("line_or_sns")
    if portal_listing and not allow_portal_listing:
        exclusion_reasons.append("portal_listing")
    if unsuitable_content:
        exclusion_reasons.append("unsuitable_content")
    if weak_contact and not allow_weak_contact:
        exclusion_reasons.append("weak_contact")
    if name_rank < min_name_rank:
        exclusion_reasons.append("low_name_confidence")
    if quality_score < min_quality_score:
        exclusion_reasons.append("below_min_quality_score")
    if not location_match:
        exclusion_reasons.append("location_mismatch")

    hard_exclusion_reasons: list[str] = []
    if blocklisted and not include_blocklisted:
        hard_exclusion_reasons.append("blocklist_domain")
    if cooldown and not include_cooldown_domains:
        hard_exclusion_reasons.append("cooldown_domain")
    if duplicate_ledger:
        hard_exclusion_reasons.append("duplicate_ledger")
    if feedback_hard_exclude:
        hard_exclusion_reasons.append(f"feedback_exclude:{feedback_exclusion or 'prior_outcome'}")
    if corporate_like and not allow_corporate:
        hard_exclusion_reasons.append("corporate_like")
    if medical_like:
        hard_exclusion_reasons.append("medical_like")
    if line_sns and not allow_line_sns:
        hard_exclusion_reasons.append("line_or_sns")
    if portal_listing and not allow_portal_listing:
        hard_exclusion_reasons.append("portal_listing")
    if unsuitable_content:
        hard_exclusion_reasons.append("unsuitable_content")
    if non_web_contact:
        hard_exclusion_reasons.append("non_web_contact")
    if not website_ok:
        hard_exclusion_reasons.append("no_usable_website")

    review_reasons: list[str] = []
    if feedback_action == "deprioritize" and not feedback_hard_exclude:
        review_reasons.append(f"feedback_deprioritize:{feedback_exclusion or 'prior_outcome'}")
    if _reason_key(failure_category) in {"external_form", "iframe_only_form"} and not hard_exclusion_reasons:
        review_reasons.append("feedback_external_form")
    if weak_contact and not any(_reason_key(reason) in {"line_or_sns", "portal_listing", "non_web_contact", "no_usable_website"} for reason in hard_exclusion_reasons):
        review_reasons.append("weak_contact")
    if toc_anchor:
        review_reasons.append("toc_anchor_contact")
    if external_contact and not hard_exclusion_reasons:
        review_reasons.append("external_contact")
    if not explicit_contact and website_ok:
        review_reasons.append("website_fallback_contact")
    if not has_contact_signal and not has_form:
        review_reasons.append("contact_unknown")
    if row["name_confidence"].lower() == "medium" and min_name_rank > _confidence_rank("medium"):
        review_reasons.append("medium_name_confidence")
    if row["name_confidence"].lower() in LOW_NAME_VALUES:
        review_reasons.append("low_name_confidence")
    elif name_rank < min_name_rank and row["name_confidence"].lower() != "medium":
        review_reasons.append("low_name_confidence")
    if quality_score < min_quality_score:
        review_reasons.append("below_min_quality_score")
    if not location_match:
        review_reasons.append("location_mismatch")

    exclusion_reasons = _dedupe(exclusion_reasons)
    hard_exclusion_reasons = _dedupe(hard_exclusion_reasons)
    review_reasons = _dedupe(review_reasons)
    lead_tier = "C" if hard_exclusion_reasons else "B" if review_reasons else "A"
    recommended_action = {
        "A": "prepare_semi_auto",
        "B": "manual_review",
        "C": "exclude_or_low_priority",
    }[lead_tier]

    contact_quality = "direct" if direct_contact else "external" if external_contact else "non_web" if non_web_contact else "unknown"
    tier_b_decision, tier_b_reason, tier_b_action = _tier_b_promotion_policy(
        lead_tier=lead_tier,
        quality_score=quality_score,
        contact_quality=contact_quality,
        contact_url=contact_url,
        contact_host=contact_host,
        direct_contact=direct_contact,
        good_contact_path=good_contact_path,
        toc_anchor=toc_anchor,
        has_form=has_form,
        review_reasons=review_reasons,
        hard_exclusion_reasons=hard_exclusion_reasons,
        failure_category=failure_category,
    )
    row["lead_tier"] = lead_tier
    row["quality_score"] = str(quality_score)
    row["exclusion_reasons"] = ";".join(hard_exclusion_reasons)
    row["review_reasons"] = ";".join(review_reasons)
    row["contact_quality"] = contact_quality
    row["name_quality"] = row["name_confidence"]
    row["tier_b_promotion_decision"] = tier_b_decision
    row["tier_b_promotion_reason"] = tier_b_reason
    row["tier_b_promotion_action"] = tier_b_action
    row["semi_auto_quality_score"] = str(quality_score)
    row["semi_auto_quality_issue"] = ";".join(_dedupe(issues))
    row["semi_auto_exclusion_reason"] = ";".join(exclusion_reasons)
    row["semi_auto_contact_quality"] = contact_quality
    row["semi_auto_location_match"] = "1" if location_match else "0"
    row["semi_auto_feedback_action"] = feedback_action
    row["semi_auto_feedback_penalty"] = str(feedback_penalty)
    row["semi_auto_feedback_bonus"] = str(feedback_bonus)
    row["failure_category"] = failure_category
    row["recommended_action"] = recommended_action
    if semi_auto_status == "prepared_full" and lead_tier == "A":
        row["recommended_action"] = "prepare_similar"
    row["lead_selection_bonus"] = str(feedback_bonus)
    row["lead_selection_penalty"] = str(feedback_penalty)

    audit_row = {
        "lead_id": lead_id,
        "domain": domain,
        "display_name": row["display_name"],
        "quality_score": str(quality_score),
        "selected": "1" if not exclusion_reasons else "0",
        "lead_tier": lead_tier,
        "exclusion_reason": row["semi_auto_exclusion_reason"],
        "hard_exclusion_reason": row["exclusion_reasons"],
        "review_reason": row["review_reasons"],
        "recommended_action": row["recommended_action"],
        "tier_b_promotion_decision": tier_b_decision,
        "tier_b_promotion_reason": tier_b_reason,
        "tier_b_promotion_action": tier_b_action,
        "quality_issue": row["semi_auto_quality_issue"],
        "name_confidence": row["name_confidence"],
        "contact_quality": contact_quality,
        "location_match": row["semi_auto_location_match"],
        "feedback_action": feedback_action,
        "feedback_penalty": str(feedback_penalty),
        "feedback_bonus": str(feedback_bonus),
        "failure_category": failure_category,
    }
    return CandidateEval(
        row=row,
        audit_row=audit_row,
        selected=not exclusion_reasons,
        quality_score=quality_score,
        exclusion_reasons=exclusion_reasons,
        review_reasons=review_reasons,
        hard_exclusion_reasons=hard_exclusion_reasons,
        lead_tier=lead_tier,
    )


def build_candidate_evaluations(
    *,
    input_path: Path,
    feedback_path: Path,
    kpi_path: Path | None = None,
    ledger_path: Path,
    blocklist_path: Path | None = None,
    cooldowns_path: Path | None = None,
    submissions_path: Path | None = None,
    review_queue_path: Path | None = None,
    include_ledger_domains: bool = False,
    include_blocklisted: bool = False,
    include_cooldown_domains: bool = False,
    allow_corporate: bool = False,
    allow_line_sns: bool = False,
    allow_portal_listing: bool = False,
    allow_weak_contact: bool = False,
    min_name_confidence: str = "medium",
    min_quality_score: int = 50,
    required_location_tokens: tuple[str, ...] = (),
    limit: int = 50,
) -> tuple[list[CandidateEval], Counter[str], list[str]]:
    feedback_paths = [feedback_path]
    if kpi_path:
        feedback_paths.append(kpi_path)
    feedback_by_id, feedback_by_domain = _merge_feedback_maps(feedback_paths)
    ledger_ids, ledger_domains = _load_ledger_keys(ledger_path)
    blocklist_domains = _load_blocklist_domains(blocklist_path) if blocklist_path else set()
    cooldown_domains = _load_active_cooldown_domains(cooldowns_path) if cooldowns_path else set()
    touched_paths = [path for path in (submissions_path, review_queue_path) if path]
    touched_ids, touched_domains = _load_touched_keys(touched_paths)
    ledger_ids.update(touched_ids)
    ledger_domains.update(touched_domains)
    source_rows = _read_csv(input_path)
    evaluations: list[CandidateEval] = []
    counts: Counter[str] = Counter(input_rows=len(source_rows))
    original_fields: list[str] = []
    for source_row, raw in enumerate(source_rows, 1):
        normalized = _normalize_row(raw, input_path, source_row)
        for key in normalized:
            if key.startswith("original__") and key not in original_fields:
                original_fields.append(key)
        evaluated = _evaluate_row(
            normalized,
            feedback_by_id=feedback_by_id,
            feedback_by_domain=feedback_by_domain,
            ledger_ids=ledger_ids,
            ledger_domains=ledger_domains,
            blocklist_domains=blocklist_domains,
            cooldown_domains=cooldown_domains,
            include_ledger_domains=include_ledger_domains,
            include_blocklisted=include_blocklisted,
            include_cooldown_domains=include_cooldown_domains,
            allow_corporate=allow_corporate,
            allow_line_sns=allow_line_sns,
            allow_portal_listing=allow_portal_listing,
            allow_weak_contact=allow_weak_contact,
            min_name_confidence=min_name_confidence,
            min_quality_score=min_quality_score,
            required_location_tokens=required_location_tokens,
        )
        evaluations.append(evaluated)
        for reason in evaluated.exclusion_reasons:
            counts[f"excluded_{_reason_key(reason)}"] += 1
        counts[f"tier_{evaluated.lead_tier.lower()}_count"] += 1
        for reason in evaluated.hard_exclusion_reasons:
            counts[f"tier_c_excluded_{_reason_key(reason)}"] += 1
        for reason in evaluated.review_reasons:
            counts[f"review_{_reason_key(reason)}"] += 1

    fieldnames = list(BASE_FIELDS)
    for field in QUALITY_FIELDS + original_fields:
        if field not in fieldnames:
            fieldnames.append(field)
    return evaluations, counts, fieldnames


def _sorted_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return sorted(
        rows,
        key=lambda row: (int(row.get("semi_auto_quality_score") or row.get("quality_score") or 0), _to_float(row.get("score", ""))),
        reverse=True,
    )


def build_candidates(
    *,
    input_path: Path,
    feedback_path: Path,
    kpi_path: Path | None = None,
    ledger_path: Path,
    blocklist_path: Path | None = None,
    cooldowns_path: Path | None = None,
    submissions_path: Path | None = None,
    review_queue_path: Path | None = None,
    include_ledger_domains: bool = False,
    include_blocklisted: bool = False,
    include_cooldown_domains: bool = False,
    allow_corporate: bool = False,
    allow_line_sns: bool = False,
    allow_portal_listing: bool = False,
    allow_weak_contact: bool = False,
    min_name_confidence: str = "medium",
    min_quality_score: int = 50,
    required_location_tokens: tuple[str, ...] = (),
    limit: int = 50,
) -> tuple[list[dict[str, str]], list[dict[str, str]], Counter[str], list[str]]:
    evaluations, counts, fieldnames = build_candidate_evaluations(
        input_path=input_path,
        feedback_path=feedback_path,
        kpi_path=kpi_path,
        ledger_path=ledger_path,
        blocklist_path=blocklist_path,
        cooldowns_path=cooldowns_path,
        submissions_path=submissions_path,
        review_queue_path=review_queue_path,
        include_ledger_domains=include_ledger_domains,
        include_blocklisted=include_blocklisted,
        include_cooldown_domains=include_cooldown_domains,
        allow_corporate=allow_corporate,
        allow_line_sns=allow_line_sns,
        allow_portal_listing=allow_portal_listing,
        allow_weak_contact=allow_weak_contact,
        min_name_confidence=min_name_confidence,
        min_quality_score=min_quality_score,
        required_location_tokens=required_location_tokens,
        limit=limit,
    )
    selected = _sorted_rows([item.row for item in evaluations if item.selected])
    if limit > 0:
        selected = selected[:limit]
    audit_rows = [item.audit_row for item in evaluations]
    counts["candidate_count"] = len(selected)
    return selected, audit_rows, counts, fieldnames


def _default_output_path() -> Path:
    return ROOT / "web_app" / "output" / f"next_semi_auto_candidates_{datetime.now(JST).strftime('%Y%m%d')}.csv"


def _write_report(path: Path, counts: Counter[str], output_path: Path, audit_path: Path, args: argparse.Namespace) -> None:
    lines = [
        f"# Next SEMI_AUTO Candidate Report - {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S %Z')}",
        "",
        f"- input: {args.input}",
        f"- output: {output_path}",
        f"- audit: {audit_path}",
        f"- feedback: {args.feedback}",
        f"- kpi: {args.kpi}",
        f"- submissions: {args.submissions}",
        f"- review_queue: {args.review_queue}",
        f"- ledger: {args.ledger}",
        f"- blocklist: {args.blocklist}",
        f"- cooldowns: {args.cooldowns}",
        f"- required_location_token: {', '.join(args.required_location_token) if args.required_location_token else ''}",
        "",
        "## Counts",
        "",
    ]
    for key, value in sorted(counts.items()):
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- Used local CSV artifacts only.",
            "- Did not open external pages, run Playwright, submit forms, send messages, or run FULL_AUTO.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _rows_for_tier(evaluations: list[CandidateEval], tier: str, limit: int = 0) -> list[dict[str, str]]:
    rows = _sorted_rows([item.row for item in evaluations if item.lead_tier == tier])
    if tier == "A" and limit > 0:
        return rows[:limit]
    return rows


def _top_counter_lines(counter: Counter[str], *, empty: str = "- none") -> list[str]:
    if not counter:
        return [empty]
    return [f"- {key}: {value}" for key, value in counter.most_common(10)]


def _write_tiered_report(
    path: Path,
    *,
    evaluations: list[CandidateEval],
    counts: Counter[str],
    tier_a_path: Path,
    tier_b_path: Path,
    tier_c_path: Path,
    args: argparse.Namespace,
) -> None:
    tier_counts = Counter(item.lead_tier for item in evaluations)
    hard_reason_counts: Counter[str] = Counter()
    review_reason_counts: Counter[str] = Counter()
    candidate_domains: Counter[str] = Counter()
    for item in evaluations:
        if item.lead_tier in {"A", "B"} and item.row.get("domain"):
            candidate_domains[item.row["domain"]] += 1
        for reason in item.hard_exclusion_reasons:
            hard_reason_counts[_reason_key(reason)] += 1
        for reason in item.review_reasons:
            review_reason_counts[_reason_key(reason)] += 1

    lines = [
        f"# Tiered Candidate Report - {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S %Z')}",
        "",
        f"- input: {args.input}",
        f"- feedback: {args.feedback}",
        f"- ledger: {args.ledger}",
        f"- blocklist: {args.blocklist}",
        f"- cooldowns: {args.cooldowns}",
        f"- Tier A output: {tier_a_path}",
        f"- Tier B review output: {tier_b_path}",
        f"- Tier C excluded output: {tier_c_path}",
        "",
        "## Counts",
        "",
        f"- total input rows: {counts.get('input_rows', 0)}",
        f"- Tier A SEMI_AUTO-ready: {tier_counts.get('A', 0)}",
        f"- Tier B review-before-SEMI_AUTO: {tier_counts.get('B', 0)}",
        f"- Tier C excluded/low-priority: {tier_counts.get('C', 0)}",
        f"- candidates remaining for demo generation: {tier_counts.get('A', 0)}",
        f"- candidates needing manual review: {tier_counts.get('B', 0)}",
        "",
        "## Top Exclusion Reasons",
        "",
        *_top_counter_lines(hard_reason_counts),
        "",
        "## Top Review Reasons",
        "",
        *_top_counter_lines(review_reason_counts),
        "",
        "## Top Candidate Domains",
        "",
        *_top_counter_lines(candidate_domains),
        "",
        "## Strategy",
        "",
        "- Tier A keeps strict SEMI_AUTO-ready rows with strong local signals.",
        "- Tier B keeps borderline rows visible for human review instead of silently dropping them.",
        "- Tier C is reserved for clearly unsuitable or already-touched domains.",
        "",
        "## Safety",
        "",
        "- Used local CSV artifacts only.",
        "- Did not run external search, open browsers, run Playwright, submit forms, send messages, or run FULL_AUTO.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a high-quality local candidate CSV for the next SEMI_AUTO batch.")
    parser.add_argument("--input", required=True, help="Local lead-finder CSV or normalized handoff CSV.")
    parser.add_argument("--feedback", default=str(DEFAULT_LOCAL_FEEDBACK), help="Local lead quality feedback CSV.")
    parser.add_argument("--kpi", default=str(_latest_playwright_csv("semi_auto_kpi_*.csv", DEFAULT_KPI)), help="Playwright semi_auto_kpi CSV.")
    parser.add_argument("--submissions", default=str(_latest_playwright_csv("submissions_*.csv", DEFAULT_SUBMISSIONS)), help="Playwright submissions CSV.")
    parser.add_argument("--review-queue", default=str(_latest_playwright_csv("review_queue_*.csv", DEFAULT_REVIEW_QUEUE)), help="Playwright review_queue CSV.")
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER), help="Playwright submission ledger CSV.")
    parser.add_argument("--blocklist", default=str(DEFAULT_BLOCKLIST), help="Playwright blocklist_domains.txt.")
    parser.add_argument("--cooldowns", default=str(DEFAULT_COOLDOWNS), help="Playwright domain_cooldowns.json.")
    parser.add_argument("--output", default="", help="Output candidate CSV path.")
    parser.add_argument("--audit-output", default="", help="Output audit CSV path.")
    parser.add_argument("--report", default="", help="Output markdown report path.")
    parser.add_argument("--tiered-output", action="store_true", help="Also write Tier A/B/C candidate CSVs and a tiered summary.")
    parser.add_argument(
        "--tier-a-output",
        default=str(ROOT / "web_app" / "output" / "next_candidates_tier_a.csv"),
        help="Tier A SEMI_AUTO-ready CSV path.",
    )
    parser.add_argument(
        "--tier-b-output",
        default=str(ROOT / "web_app" / "output" / "next_candidates_tier_b_review.csv"),
        help="Tier B human-review CSV path.",
    )
    parser.add_argument(
        "--tier-c-output",
        default=str(ROOT / "web_app" / "output" / "next_candidates_tier_c_excluded.csv"),
        help="Tier C excluded/low-priority CSV path.",
    )
    parser.add_argument(
        "--tiered-report",
        default=str(ROOT / "web_app" / "output" / "next_candidates_tiered.summary.md"),
        help="Tiered markdown summary path.",
    )
    parser.add_argument("--limit", type=int, default=50, help="Maximum candidates to write; 0 means no limit.")
    parser.add_argument("--min-name-confidence", choices=["low", "medium", "high"], default="medium")
    parser.add_argument("--min-quality-score", type=int, default=50)
    parser.add_argument(
        "--required-location-token",
        action="append",
        default=[],
        help="Require at least one token in name/url/location/address fields. May be repeated.",
    )
    parser.add_argument("--include-ledger-domains", action="store_true", help="Allow domains/IDs already present in ledger.")
    parser.add_argument("--include-blocklisted", action="store_true", help="Allow blocklisted domains.")
    parser.add_argument("--include-cooldown-domains", action="store_true", help="Allow domains with active cooldowns.")
    parser.add_argument("--allow-corporate", action="store_true")
    parser.add_argument("--allow-line-sns", action="store_true")
    parser.add_argument("--allow-portal-listing", action="store_true")
    parser.add_argument("--allow-weak-contact", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else _default_output_path()
    audit_path = Path(args.audit_output) if args.audit_output else output_path.with_suffix(".audit.csv")
    report_path = Path(args.report) if args.report else output_path.with_suffix(".summary.md")
    evaluations, counts, fieldnames = build_candidate_evaluations(
        input_path=input_path,
        feedback_path=Path(args.feedback),
        kpi_path=Path(args.kpi),
        ledger_path=Path(args.ledger),
        blocklist_path=Path(args.blocklist),
        cooldowns_path=Path(args.cooldowns),
        submissions_path=Path(args.submissions),
        review_queue_path=Path(args.review_queue),
        include_ledger_domains=bool(args.include_ledger_domains),
        include_blocklisted=bool(args.include_blocklisted),
        include_cooldown_domains=bool(args.include_cooldown_domains),
        allow_corporate=bool(args.allow_corporate),
        allow_line_sns=bool(args.allow_line_sns),
        allow_portal_listing=bool(args.allow_portal_listing),
        allow_weak_contact=bool(args.allow_weak_contact),
        min_name_confidence=str(args.min_name_confidence),
        min_quality_score=int(args.min_quality_score),
        required_location_tokens=tuple(str(token) for token in args.required_location_token if str(token).strip()),
        limit=int(args.limit),
    )
    selected = _sorted_rows([item.row for item in evaluations if item.selected])
    if int(args.limit) > 0:
        selected = selected[: int(args.limit)]
    audit_rows = [item.audit_row for item in evaluations]
    counts["candidate_count"] = len(selected)
    audit_fields = [
        "lead_id",
        "domain",
        "display_name",
        "quality_score",
        "selected",
        "lead_tier",
        "exclusion_reason",
        "hard_exclusion_reason",
        "review_reason",
        "recommended_action",
        "tier_b_promotion_decision",
        "tier_b_promotion_reason",
        "tier_b_promotion_action",
        "quality_issue",
        "name_confidence",
        "contact_quality",
        "location_match",
        "feedback_action",
        "feedback_penalty",
        "feedback_bonus",
        "failure_category",
    ]
    _write_csv(output_path, fieldnames, selected)
    _write_csv(audit_path, audit_fields, audit_rows)
    _write_report(report_path, counts, output_path, audit_path, args)
    if args.tiered_output:
        tier_a_path = Path(args.tier_a_output)
        tier_b_path = Path(args.tier_b_output)
        tier_c_path = Path(args.tier_c_output)
        tiered_report_path = Path(args.tiered_report)
        tier_a_rows = _rows_for_tier(evaluations, "A", int(args.limit))
        tier_b_rows = _rows_for_tier(evaluations, "B")
        tier_c_rows = _rows_for_tier(evaluations, "C")
        _write_csv(tier_a_path, fieldnames, tier_a_rows)
        _write_csv(tier_b_path, fieldnames, tier_b_rows)
        _write_csv(tier_c_path, fieldnames, tier_c_rows)
        _write_tiered_report(
            tiered_report_path,
            evaluations=evaluations,
            counts=counts,
            tier_a_path=tier_a_path,
            tier_b_path=tier_b_path,
            tier_c_path=tier_c_path,
            args=args,
        )
    print(f"input_rows={counts.get('input_rows', 0)}")
    for key in sorted(k for k in counts if k.startswith("excluded_")):
        print(f"{key}={counts[key]}")
    print(f"candidate_count={counts.get('candidate_count', 0)}")
    if args.tiered_output:
        print(f"tier_a_count={counts.get('tier_a_count', 0)}")
        print(f"tier_b_count={counts.get('tier_b_count', 0)}")
        print(f"tier_c_count={counts.get('tier_c_count', 0)}")
        print(f"tier_a_output={Path(args.tier_a_output)}")
        print(f"tier_b_output={Path(args.tier_b_output)}")
        print(f"tier_c_output={Path(args.tier_c_output)}")
        print(f"tiered_report={Path(args.tiered_report)}")
    print(f"output={output_path}")
    print(f"audit={audit_path}")
    print(f"report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
