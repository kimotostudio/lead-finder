#!/usr/bin/env python3
"""Normalize lead-finder CSV output for demo and review-first outreach handoff.

This script only transforms local CSV files. It does not fetch websites, open a
browser, generate demos, or submit forms.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

try:
    from tools.display_name_cleaner import clean_display_name
except ModuleNotFoundError:
    from display_name_cleaner import clean_display_name


NORMALIZED_FIELDS = [
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
    "form_url",
    "industry",
    "business_type",
    "location",
    "area",
    "address",
    "score",
    "solo_score",
    "notes",
    "domain",
    "contact_page_has_form",
    "contact_page_address",
    "canonical_contact_url",
    "canonical_contact_path",
    "contact_path_candidates",
    "contact_path_ambiguity",
    "form_evidence_kind",
    "identity_signal",
    "single_location_evidence",
    "address_evidence_source",
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

FUKUOKA_CITY_WARDS = (
    "中央区",
    "博多区",
    "東区",
    "南区",
    "西区",
    "城南区",
    "早良区",
)
LOCAL_ADDRESS_RE = re.compile(
    r"(?:〒\s*\d{3}-?\d{4}\s*)?(?:福岡県\s*)?福岡市\s*(?:"
    + "|".join(re.escape(ward) for ward in FUKUOKA_CITY_WARDS)
    + r")[^\n\r。|｜]{0,100}"
)
CONTACT_EVIDENCE_KEYS = (
    "contact_fetch_status",
    "contact_fetch_error",
    "contact_page_title",
    "contact_page_address",
    "contact_page_has_form",
)
CONTACT_PATH_TOKENS = ("contact", "inquiry", "otoiawase", "toiawase", "お問い合わせ", "問合せ")
RESERVATION_PATH_TOKENS = ("reservation", "reserve", "booking", "book", "yoyaku", "予約", "ご予約")
CORPORATE_SIGNAL_TOKENS = ("corporate", "株式会社", "有限会社", "合同会社", "inc", "llc")
SOLO_SIGNAL_TOKENS = ("solo", "個人", "ひとり", "一人")


NAME_FIELDS = [
    "display_name",
    "business_name",
    "salon_name",
    "brand_name",
    "store_name",
    "shop_name",
    "company_name",
    "name",
    "site_name",
    "title",
    "店名",
    "名称",
    "サロン名",
    "店舗名",
]

LOW_CONFIDENCE_NAME_FIELDS = {"title", "original__title", "site_title"}
NOISY_NAME_TOKENS = [
    "google",
    "口コミ",
    "レビュー",
    "評価",
    "ランキング",
    "地図",
    "住所",
    "電話番号",
    "営業時間",
    "アクセス",
    "予約",
    "メニュー",
    "料金",
    "価格",
    "提供",
    "必要な時",
    "気になる部分",
    "改善",
    "技術力",
    "空間",
    "内容",
    "福岡市",
    "福岡",
    "中央区",
    "博多区",
    "南区",
    "早良区",
    "東区",
    "西区",
    "城南区",
    "公式サイト",
    "公式ホームページ",
    "ホームページ",
    "検索サイト",
    "情報",
    "〒",
]
CATEGORY_ONLY_TOKENS = [
    "整体",
    "エステ",
    "脱毛",
    "美容室",
    "美容院",
    "サロン",
    "鍼灸",
    "整骨院",
    "接骨院",
    "マッサージ",
    "ネイル",
    "まつげ",
    "まつ毛",
    "パーソナルジム",
    "メンズエステ",
    "首肩こり",
    "HOME",
    "ホーム",
    "地元の公司を見つける",
    "骨盤",
    "首こり首の痛み",
    "最寄り駅大濠公園駅",
]

WEBSITE_FIELDS = [
    "website",
    "url",
    "reference_url",
    "old_url",
    "url(旧)",
    "URL",
]

CONTACT_FIELDS = [
    "contact_url",
    "contact_page",
    "form_url",
    "inquiry_url",
    "お問い合わせURL",
]

AREA_FIELDS = ["area", "area_guess", "location", "address", "city", "prefecture"]
SCORE_FIELDS = ["score", "lead_score", "weakness_score", "solo_score"]
TYPE_FIELDS = ["business_type", "industry", "category", "category_guess"]
NOTES_FIELDS = ["notes", "reason", "ai_filter_reason", "weakness_reasons"]
IGNORED_SCHEMES = {"tel", "mailto", "line", "sms", "javascript", "data"}
DOMAIN_RE = re.compile(
    r"^(?:www\.)?[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$",
    re.IGNORECASE,
)


def _norm_key(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("\ufeff", "")
    text = text.replace("（", "(").replace("）", ")")
    text = text.replace("　", " ")
    return re.sub(r"\s+", "", text)


def _pick(row: dict[str, str], candidates: Iterable[str]) -> str:
    normalized = {_norm_key(k): v for k, v in row.items()}
    for key in candidates:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
        value = normalized.get(_norm_key(key))
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _pick_with_source(row: dict[str, str], candidates: Iterable[str]) -> tuple[str, str]:
    normalized = {_norm_key(k): (k, v) for k, v in row.items()}
    for key in candidates:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip(), key
        alt = normalized.get(_norm_key(key))
        if alt is not None and str(alt[1]).strip():
            return str(alt[1]).strip(), str(alt[0])
    return "", ""


def _clean_spaces(value: str) -> str:
    text = str(value or "").replace("\ufeff", "").replace("\u200b", "")
    text = text.replace("　", " ")
    text = re.sub(r"\s+", " ", text).strip(" -_|｜:：／/")
    return text


def _truth_text(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "あり", "有"}:
        return "True"
    if text in {"0", "false", "no", "n", "なし", "無"}:
        return "False"
    return ""


def _same_site_url(url: str, domain: str) -> bool:
    parsed = urlparse(str(url or "").strip())
    host = _clean_domain(parsed.netloc)
    target = _clean_domain(domain)
    return bool(
        host
        and target
        and (host == target or host.endswith(f".{target}") or target.endswith(f".{host}"))
    )


def _url_path(value: str) -> str:
    parsed = urlparse(str(value or "").strip())
    path = parsed.path or "/"
    return path.rstrip("/") or "/"


def _plain_contact_path(value: str) -> bool:
    path = _url_path(value).lower()
    return any(token.lower() in path for token in CONTACT_PATH_TOKENS)


def _reservation_path(value: str) -> bool:
    path = _url_path(value).lower()
    return any(token.lower() in path for token in RESERVATION_PATH_TOKENS)


def _extract_contact_evidence(row: dict[str, str]) -> dict[str, str]:
    text = _pick(row, ["reason", "notes", "original__reason"])
    evidence: dict[str, str] = {}
    if not text:
        return evidence
    key_pattern = "|".join(re.escape(key) for key in CONTACT_EVIDENCE_KEYS)
    pattern = re.compile(
        rf"(?:^|\s\|\s)({key_pattern})=(.*?)(?=\s\|\s(?:{key_pattern})=|$)"
    )
    for match in pattern.finditer(text):
        evidence[match.group(1)] = _clean_spaces(match.group(2))
    return evidence


def _extract_local_address(text: str) -> str:
    haystack = re.sub(r"\s+", " ", str(text or ""))
    match = LOCAL_ADDRESS_RE.search(haystack)
    if match:
        return match.group(0).strip(" 、,")
    for ward in FUKUOKA_CITY_WARDS:
        token = f"福岡市{ward}"
        if token in haystack:
            return token
    return ""


def _ward_hit(text: str) -> str:
    for ward in FUKUOKA_CITY_WARDS:
        if f"福岡市{ward}" in str(text or ""):
            return ward
    return ""


def _normalize_address_evidence(row: dict[str, str], contact_evidence: dict[str, str]) -> dict[str, str]:
    raw_address = _pick(row, ["address", "original__address"])
    contact_page_address = _extract_local_address(contact_evidence.get("contact_page_address", ""))
    title_address = _extract_local_address(_pick(row, ["title", "original__title"]))
    raw_local_address = _extract_local_address(raw_address)

    if raw_local_address:
        address = raw_address
        source = "source_address"
    elif contact_page_address:
        address = contact_page_address
        source = "contact_page_address"
    elif title_address:
        address = title_address
        source = "title_address"
    else:
        address = raw_address
        source = "source_address" if raw_address else ""

    ward = _ward_hit(address)
    single_location = f"fukuoka_ward_address:{ward}" if ward else ""
    return {
        "address": address,
        "contact_page_address": contact_page_address or contact_evidence.get("contact_page_address", ""),
        "address_evidence_source": source,
        "single_location_evidence": single_location,
        "raw_address": raw_address,
    }


def _contact_candidates(row: dict[str, str]) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    for field in ("contact_url", "contact_page", "form_url", "inquiry_url", "お問い合わせURL"):
        value = _pick(row, [field])
        if value:
            candidates.append((field, value))
    deduped: list[tuple[str, str]] = []
    seen: set[str] = set()
    for label, value in candidates:
        key = value.strip()
        if key and key not in seen:
            seen.add(key)
            deduped.append((label, key))
    return deduped


def _normalize_contact_evidence(
    row: dict[str, str],
    *,
    domain: str,
    contact_evidence: dict[str, str],
) -> dict[str, str]:
    candidates = _contact_candidates(row)
    raw_contact_url = _pick(row, ["contact_url", "contact_page"])
    raw_form_url = _pick(row, ["form_url", "inquiry_url", "お問い合わせURL"])
    contact_has_form = _truth_text(contact_evidence.get("contact_page_has_form", ""))
    raw_has_form = _truth_text(_pick(row, ["has_form", "original__has_form"]))

    def score(item: tuple[str, str]) -> tuple[int, int, int, int, str]:
        label, value = item
        same_site = _same_site_url(value, domain)
        plain_contact = _plain_contact_path(value)
        reservation = _reservation_path(value)
        confirmed_contact_page = bool(contact_has_form == "True" and value == raw_contact_url)
        return (
            1 if same_site else 0,
            1 if confirmed_contact_page else 0,
            1 if plain_contact and not reservation else 0,
            1 if label in {"contact_url", "contact_page"} else 0,
            value,
        )

    canonical_contact = ""
    if candidates:
        canonical_contact = max(candidates, key=score)[1]

    same_site_paths: dict[str, str] = {}
    for label, value in candidates:
        if _same_site_url(value, domain) and _plain_contact_path(value):
            same_site_paths.setdefault(_url_path(value), f"{label}={value}")
    path_candidates = ";".join(same_site_paths.values())
    ambiguity = ""
    if len(same_site_paths) > 1:
        ambiguity = "multiple_same_site_contact_paths:" + path_candidates

    if contact_has_form == "True":
        normalized_has_form = "True"
        canonical_form_url = canonical_contact or raw_contact_url or raw_form_url
        form_evidence_kind = "confirmed_same_site_contact_page_form"
    elif contact_has_form == "False":
        normalized_has_form = "False"
        canonical_form_url = raw_form_url
        form_evidence_kind = "confirmed_same_site_contact_page_no_form"
    elif raw_has_form == "True" and raw_form_url:
        normalized_has_form = "True"
        canonical_form_url = raw_form_url
        form_evidence_kind = "form_like_link_unconfirmed"
    elif raw_has_form:
        normalized_has_form = raw_has_form
        canonical_form_url = raw_form_url
        form_evidence_kind = "raw_has_form_unconfirmed"
    else:
        normalized_has_form = ""
        canonical_form_url = raw_form_url
        form_evidence_kind = ""

    return {
        "contact_url": canonical_contact or raw_contact_url,
        "contact_page": canonical_contact or raw_contact_url,
        "form_url": canonical_form_url,
        "has_form": normalized_has_form,
        "contact_page_has_form": contact_has_form,
        "canonical_contact_url": canonical_contact or raw_contact_url,
        "canonical_contact_path": _url_path(canonical_contact or raw_contact_url) if (canonical_contact or raw_contact_url) else "",
        "contact_path_candidates": path_candidates,
        "contact_path_ambiguity": ambiguity,
        "form_evidence_kind": form_evidence_kind,
        "raw_contact_url": raw_contact_url,
        "raw_form_url": raw_form_url,
        "raw_has_form": _pick(row, ["has_form", "original__has_form"]),
    }


def _identity_signal(row: dict[str, str]) -> str:
    text = " ".join(
        [
            _pick(row, ["reason", "notes", "original__reason"]),
            _pick(row, ["display_name", "business_name", "company_name", "title", "original__title"]),
        ]
    ).lower()
    if any(token.lower() in text for token in CORPORATE_SIGNAL_TOKENS):
        return "corporate"
    if any(token.lower() in text for token in SOLO_SIGNAL_TOKENS):
        return "solo"
    return ""


def _strip_title_noise(value: str) -> str:
    text = _clean_spaces(value)
    text = re.sub(r"\s*[-|｜]\s*(公式|ホームページ|オフィシャルサイト|Official Site).*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*(?:公式サイト|公式ホームページ|ホームページ)$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^(公式|ホームページ)\s*[:：-]?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^(?:HOME|ホーム)\s*[|｜│]\s*", "", text, flags=re.IGNORECASE)
    return _clean_spaces(text)


def _name_warnings(value: str) -> list[str]:
    warnings: list[str] = []
    text = str(value or "").strip()
    lowered = text.lower()
    if not text:
        warnings.append("missing")
        return warnings
    if len(text) > 32:
        warnings.append("long_name")
    if any(token.lower() in lowered for token in NOISY_NAME_TOKENS):
        warnings.append("title_or_location_noise")
    if "駅" in text and len(text) <= 16:
        warnings.append("title_or_location_noise")
    compact = re.sub(r"\s+", "", text)
    if compact in CATEGORY_ONLY_TOKENS:
        warnings.append("category_only")
    if compact.upper() in {"HOME", "TOP"}:
        warnings.append("generic_title")
    if len(compact) <= 8 and any(token in compact for token in CATEGORY_ONLY_TOKENS):
        warnings.append("category_only")
    if re.search(r"\d+\.\d|\d+件|\d+\s* reviews?", lowered):
        warnings.append("review_or_rating_noise")
    if re.search(r"\d{2,4}[-ー]\d{2,4}", text):
        warnings.append("phone_or_address_noise")
    if any(token in text for token in ["Ã", "Â", "ã", "å", "ç", "ä¸", "", "", ""]):
        warnings.append("mojibake")
    return warnings


def _score_name(value: str, source: str) -> tuple[int, str, list[str]]:
    cleaned = _strip_title_noise(value)
    warnings = _name_warnings(cleaned)
    score = 90
    if source in LOW_CONFIDENCE_NAME_FIELDS:
        score -= 20
    if "long_name" in warnings:
        score -= 25
    if "title_or_location_noise" in warnings:
        score -= 25
    if "category_only" in warnings:
        score -= 35
    if "generic_title" in warnings:
        score -= 50
    if "review_or_rating_noise" in warnings or "phone_or_address_noise" in warnings:
        score -= 35
    if "mojibake" in warnings:
        score -= 80
    if not cleaned:
        score = 0
    confidence = "high" if score >= 80 else "medium" if score >= 55 else "low"
    return score, confidence, warnings


def _title_name_variants(value: str) -> list[str]:
    text = _strip_title_noise(value)
    variants = [text]
    for delimiter in ["|", "｜", "│", " - ", " – ", " — ", "／", "/"]:
        if delimiter in text:
            variants.extend(_clean_spaces(part) for part in text.split(delimiter) if _clean_spaces(part))
    for marker in ["なら", "ならば"]:
        if marker in text:
            tail = _clean_spaces(text.rsplit(marker, 1)[-1])
            if tail:
                variants.append(tail)
    for pattern in [
        r"(?:専門店|専門サロン|プライベートエステサロン|サロン)\s*([A-Za-z][A-Za-z0-9&.' /・（）()　-]{2,})",
        r"([一-龥ぁ-んァ-ヶーA-Za-zＡ-Ｚａ-ｚ0-9０-９&.' 　-]{2,}(?:鍼灸院|整体|サロン|ケアルーム|ボディケアルーム))",
        r"((?:鍼灸|美容整体|りらく|出張マッサージ)?[一-龥ぁ-んァ-ヶーA-Za-zＡ-Ｚａ-ｚ0-9０-９&.' 　-]{2,}(?:縁らく|福岡中央整体|SOAR|basil))",
        r"(?:なら|ならば)\s*([^|｜│／/]+)$",
        r"(?:なら|ならば)\s*([^|｜│／/]+)",
        r"(?:の|で)(?:脱毛|まつげ|まつ毛|眉毛|アイブロウ|マッサージ|エステ|整体|鍼灸|美容鍼灸)(?:専門店|専門サロン|サロン|院)?\s*([A-Za-zＡ-Ｚａ-ｚ0-9０-９一-龥ぁ-んァ-ヶー&.' ・（）()　-]{2,})",
    ]:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            candidate = _clean_spaces(match.group(1))
            if candidate:
                variants.append(candidate)
    if "・" in text:
        parts = [_clean_spaces(part) for part in text.split("・") if _clean_spaces(part)]
        variants.extend(part for part in parts if 2 <= len(part) <= 24)
        for part in parts:
            if re.search(r"(鍼灸院|整体|サロン|ケアルーム)$", part):
                variants.append(part)
    stripped_prefix = re.sub(
        r"^(?:HOME|ホーム|公式|福岡市中央区|福岡市|福岡|天神|薬院|大名|平尾|高宮駅近く|大濠公園駅近く|大濠公園|中央区|博多区|南区|早良区|東区|西区|城南区)[\s　・／/-]*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    if stripped_prefix and stripped_prefix != text:
        variants.append(_clean_spaces(stripped_prefix))
    station_near = re.search(r"駅(?:近く|近|周辺)の(.+)$", text)
    if station_near:
        variants.append(_clean_spaces(station_near.group(1)))
    location_de = re.search(r"(?:市|区|町|村|駅)で(.+?)(?:なら|$)", text)
    if location_de:
        variants.append(_clean_spaces(location_de.group(1)))
    for match in re.finditer(r"[A-Z][A-Za-z0-9&.' -]{2,}", text):
        candidate = _clean_spaces(match.group(0))
        if candidate:
            variants.append(candidate)
    deduped: list[str] = []
    for candidate in variants:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def resolve_display_name(row: dict[str, str], domain: str) -> dict[str, str]:
    candidates: list[tuple[str, str]] = []
    for field in NAME_FIELDS:
        value, source = _pick_with_source(row, [field])
        if not value:
            continue
        if source in LOW_CONFIDENCE_NAME_FIELDS or _norm_key(source) in {_norm_key(k) for k in LOW_CONFIDENCE_NAME_FIELDS}:
            for variant in _title_name_variants(value):
                candidates.append((variant, source))
        else:
            candidates.append((_strip_title_noise(value), source))

    best_value = ""
    best_source = ""
    best_score = -1
    best_confidence = "low"
    best_warnings: list[str] = []
    for value, source in candidates:
        score, confidence, warnings = _score_name(value, source)
        if score > best_score:
            best_value = value
            best_source = source
            best_score = score
            best_confidence = confidence
            best_warnings = warnings

    if not best_value and domain:
        return {
            "display_name": domain,
            "name_confidence": "low",
            "name_source": "domain",
            "name_warning": "domain_fallback",
        }

    warning_text = ";".join(best_warnings)
    if best_confidence == "low" and not warning_text:
        warning_text = "uncertain_name"
    return {
        "display_name": best_value,
        "name_confidence": best_confidence,
        "name_source": best_source,
        "name_warning": warning_text,
    }


def _clean_domain(value: str) -> str:
    domain = str(value or "").strip().lower()
    if not domain:
        return ""
    if "@" in domain:
        domain = domain.rsplit("@", 1)[-1]
    if ":" in domain:
        domain = domain.split(":", 1)[0]
    domain = domain.strip(".")
    if domain.startswith("www."):
        domain = domain[4:]
    if DOMAIN_RE.match(domain):
        return domain
    return ""


def _extract_domain(*urls: str) -> str:
    for raw in urls:
        value = str(raw or "").strip()
        if not value:
            continue

        scheme_match = re.match(r"^([a-z][a-z0-9+.-]*):", value, re.IGNORECASE)
        if scheme_match and scheme_match.group(1).lower() in IGNORED_SCHEMES:
            continue

        if re.match(r"^https?://", value, re.IGNORECASE):
            parsed = urlparse(value)
            domain = _clean_domain(parsed.netloc)
        elif value.lower().startswith("www."):
            domain = _clean_domain(value.split("/", 1)[0])
        else:
            domain = _clean_domain(value.split("/", 1)[0])

        if domain:
            return domain
    return ""


def _slug(value: str, *, max_length: int = 48) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^\w]+", "-", text, flags=re.UNICODE)
    text = re.sub(r"-+", "-", text).strip("-_")
    return text[:max_length].strip("-_")


def _generated_lead_id(*, domain: str, business_name: str, source_row: int) -> str:
    domain_slug = _slug(domain.replace(".", "-"), max_length=56)
    if domain_slug:
        return f"lf-{domain_slug}"

    name_slug = _slug(business_name, max_length=40)
    if name_slug:
        return f"lf-{name_slug}-r{source_row:05d}"

    return f"lf-unknown-r{source_row:05d}"


def _default_output_path(input_path: Path) -> Path:
    stem = input_path.stem
    if stem.startswith("outreach_ready_"):
        stem = stem.replace("outreach_ready_", "handoff_normalized_", 1)
    else:
        stem = f"handoff_normalized_{stem}"
    return input_path.with_name(f"{stem}.csv")


def normalize_row(row: dict[str, str], *, source_csv: str, source_row: int) -> dict[str, str]:
    website = _pick(row, WEBSITE_FIELDS)
    contact_url = _pick(row, CONTACT_FIELDS)
    domain = _extract_domain(_pick(row, ["domain"]), website, contact_url)
    contact_evidence = _extract_contact_evidence(row)
    contact_normalized = _normalize_contact_evidence(
        row,
        domain=domain,
        contact_evidence=contact_evidence,
    )
    address_normalized = _normalize_address_evidence(row, contact_evidence)
    identity_signal = _identity_signal(row)
    display = clean_display_name(row, domain)
    display_name = display.display_name
    raw_business_name = _pick(row, ["business_name", "salon_name", "brand_name", "company_name", "name", "title", "店名", "名称"])
    business_name = display_name or raw_business_name or domain
    lead_id = _pick(row, ["lead_id", "id", "ID", "管理番号"])
    if not lead_id:
        lead_id = _generated_lead_id(domain=domain, business_name=business_name, source_row=source_row)

    business_type = _pick(row, TYPE_FIELDS)
    area = _pick(row, AREA_FIELDS)
    score = _pick(row, SCORE_FIELDS)
    notes = _pick(row, NOTES_FIELDS)
    canonical_contact_url = contact_normalized["contact_url"]

    normalized = {
        "lead_id": lead_id,
        "id": lead_id,
        "company_name": business_name,
        "business_name": business_name,
        "display_name": display_name,
        "salon_name": business_name,
        "brand_name": business_name,
        "name_confidence": display.name_confidence,
        "name_source": display.name_source,
        "name_warning": display.name_warning,
        "original_display_name": display.original_display_name,
        "original_title": display.original_title,
        "website": website,
        "url": website,
        "reference_url": website,
        "contact_page": contact_normalized["contact_page"],
        "contact_url": canonical_contact_url,
        "form_url": contact_normalized["form_url"],
        "industry": _pick(row, ["industry"]) or business_type,
        "business_type": business_type,
        "location": _pick(row, ["location"]) or area,
        "area": area,
        "address": address_normalized["address"],
        "score": score,
        "solo_score": _pick(row, ["solo_score"]) or score,
        "notes": notes,
        "domain": domain,
        "contact_page_has_form": contact_normalized["contact_page_has_form"],
        "contact_page_address": address_normalized["contact_page_address"],
        "canonical_contact_url": contact_normalized["canonical_contact_url"],
        "canonical_contact_path": contact_normalized["canonical_contact_path"],
        "contact_path_candidates": contact_normalized["contact_path_candidates"],
        "contact_path_ambiguity": contact_normalized["contact_path_ambiguity"],
        "form_evidence_kind": contact_normalized["form_evidence_kind"],
        "identity_signal": identity_signal,
        "single_location_evidence": address_normalized["single_location_evidence"],
        "address_evidence_source": address_normalized["address_evidence_source"],
        "demo_path": "",
        "demo_url": "",
        "message_path": "",
        "message": "",
        "status": "",
        "reason": "",
        "source_csv": source_csv,
        "source_row": str(source_row),
        "template": "",
        "image": "",
        "therapist_image": "",
        "url(旧)": website,
        "url(デモ)": "",
        "店名": business_name,
    }

    for key, value in row.items():
        clean_key = str(key or "").replace("\ufeff", "").strip()
        if not clean_key:
            continue
        original_key = f"original__{clean_key}"
        if original_key not in normalized:
            normalized[original_key] = str(value or "").strip()

    # Downstream local-service gates consume these original__ fields directly.
    # Normalize only evidence that was already present in the local source row,
    # while retaining raw values in adjacent audit fields.
    normalized["original__address"] = address_normalized["address"]
    normalized["original__raw_address"] = address_normalized["raw_address"]
    normalized["original__contact_page_address"] = address_normalized["contact_page_address"]
    normalized["original__address_evidence_source"] = address_normalized["address_evidence_source"]
    normalized["original__single_location_evidence"] = address_normalized["single_location_evidence"]
    normalized["original__contact_url"] = contact_normalized["raw_contact_url"] or canonical_contact_url
    normalized["original__form_url"] = contact_normalized["form_url"]
    normalized["original__raw_form_url"] = contact_normalized["raw_form_url"]
    normalized["original__raw_has_form"] = contact_normalized["raw_has_form"]
    normalized["original__has_form"] = contact_normalized["has_form"]
    normalized["original__contact_page_has_form"] = contact_normalized["contact_page_has_form"]
    normalized["original__form_evidence_kind"] = contact_normalized["form_evidence_kind"]
    normalized["original__canonical_contact_url"] = contact_normalized["canonical_contact_url"]
    normalized["original__canonical_contact_path"] = contact_normalized["canonical_contact_path"]
    normalized["original__contact_path_candidates"] = contact_normalized["contact_path_candidates"]
    normalized["original__contact_path_ambiguity"] = contact_normalized["contact_path_ambiguity"]
    normalized["original__identity_signal"] = identity_signal

    return normalized


def convert(input_path: Path, output_path: Path) -> tuple[int, list[str]]:
    with input_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        source_fields = [str(field or "").replace("\ufeff", "").strip() for field in (reader.fieldnames or [])]
        rows = [
            normalize_row(row, source_csv=str(input_path), source_row=index)
            for index, row in enumerate(reader, start=1)
        ]

    original_fields = [f"original__{field}" for field in source_fields if field]
    fieldnames = list(NORMALIZED_FIELDS)
    for field in original_fields:
        if field not in fieldnames:
            fieldnames.append(field)
    for row in rows:
        for field in row:
            if field not in fieldnames:
                fieldnames.append(field)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return len(rows), fieldnames


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a lead-finder CSV into a normalized local handoff CSV."
    )
    parser.add_argument("--input", "-i", required=True, help="Input lead-finder CSV path")
    parser.add_argument(
        "--output",
        "-o",
        default="",
        help="Output CSV path. Defaults next to input as handoff_normalized_*.csv",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.is_file():
        raise SystemExit(f"Input CSV not found: {input_path}")

    output_path = Path(args.output) if args.output else _default_output_path(input_path)
    row_count, fieldnames = convert(input_path, output_path)
    print(f"wrote {row_count} rows to {output_path}")
    print(",".join(fieldnames))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
