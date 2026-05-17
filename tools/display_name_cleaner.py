#!/usr/bin/env python3
"""Clean store/business display names from local lead CSV rows.

This module is deliberately local-only: it parses already captured CSV fields
and does not fetch websites, open browsers, or contact external services.
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse


REVIEW_FIELDS = [
    "lead_id",
    "domain",
    "original_display_name",
    "cleaned_display_name",
    "name_confidence",
    "name_source",
    "name_warning",
    "title",
    "website",
    "contact_url",
    "tier",
    "suggested_action",
]

OUTPUT_NAME_FIELDS = ["company_name", "business_name", "display_name", "salon_name", "brand_name", "店名"]
EXPLICIT_NAME_FIELDS = [
    "store_name",
    "shop_name",
    "店舗名",
    "店名",
    "名称",
    "サロン名",
    "business_name",
    "salon_name",
    "brand_name",
    "company_name",
    "name",
    "display_name",
]
SITE_NAME_FIELDS = [
    "og_site_name",
    "og:site_name",
    "site_name",
    "original__og_site_name",
    "original__site_name",
]
TITLE_FIELDS = [
    "original_title",
    "title",
    "site_title",
    "og_title",
    "original__title",
    "original__original__title",
    "original__site_title",
    "original__og_title",
]
DOMAIN_FIELDS = ["domain", "original__domain", "website", "url", "reference_url", "contact_url", "contact_page"]
TITLE_DERIVED_SOURCES = {"title", "site_title", "original__title", "original__original__title", "title_cleaned"}
DOMAIN_SOURCES = {"domain", "domain_fallback"}

GENERIC_TITLES = {
    "top",
    "top page",
    "toppage",
    "home",
    "home page",
    "homepage",
    "lesson",
    "contact",
    "about",
    "music room",
    "トップ",
    "トップページ",
    "ホーム",
    "ホームページ",
    "お知らせ",
    "ブログ",
    "メニュー",
    "アクセス",
    "予約",
    "お問い合わせ",
}
PROMOTIONAL_TOKENS = [
    "医師・専門家が絶賛",
    "専門家が絶賛",
    "無料体験",
    "無料体験レッスン",
    "実施中",
    "キャンペーン",
    "初回限定",
    "今だけ",
    "口コミ",
    "レビュー",
    "ランキング",
    "おすすめ",
    "人気",
    "選ばれる",
]
CATEGORY_TOKENS = [
    "ヨガ",
    "ホットヨガ",
    "ピラティス",
    "エアリアルヨガ",
    "整体",
    "整骨院",
    "接骨院",
    "鍼灸",
    "エステ",
    "脱毛",
    "美容室",
    "美容院",
    "サロン",
    "音楽教室",
    "音楽スクール",
    "ピアノレッスン",
    "リトミック",
    "レッスン",
    "スタジオ",
    "フィットネス",
    "スクール",
]
LOCATION_TOKENS = [
    "福岡市",
    "福岡",
    "中央区",
    "博多区",
    "南区",
    "早良区",
    "東区",
    "西区",
    "城南区",
    "天神",
    "薬院",
    "大名",
    "平尾",
    "駅",
    "丁目",
    "〒",
]
BUSINESS_SUFFIXES = [
    "サロン",
    "整骨院",
    "接骨院",
    "整体院",
    "鍼灸院",
    "スタジオ",
    "スクール",
    "教室",
    "ルーム",
    "room",
    "studio",
    "salon",
]
NOISE_STRIP_PATTERNS = [
    r"\s*(?:公式サイト|公式ホームページ|オフィシャルサイト|Official Site)\s*$",
    r"^(?:公式|ホームページ|HOME|ホーム)\s*[:：|｜/\-]*\s*",
]
TITLE_PREFIX_PATTERNS = [
    r"^(?:福岡市|福岡|天神|薬院|大名|博多|中央区|南区|早良区|東区|西区|城南区)(?:で|の|・|/|\s)*",
    r"^(?:音楽スクール|音楽教室|ヨガ(?:・フィットネス)?スタジオ|フィットネススタジオ|整体|整骨院|サロン)\s*",
]
TITLE_SEPARATORS_RE = re.compile(r"\s*(?:\|\||\||｜|│|::|：|／|/| – | — | - )\s*")
JP_PUBLIC_SUFFIX_SECOND_LEVELS = {"co", "ne", "or", "ac", "go", "lg", "ed", "gr"}


@dataclass(frozen=True)
class NameResult:
    display_name: str
    name_confidence: str
    name_source: str
    name_warning: str
    original_display_name: str
    original_title: str
    score: int


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


def _pick_with_source(row: dict[str, str], keys: Iterable[str]) -> tuple[str, str]:
    normalized = {_norm_key(k): (k, v) for k, v in row.items()}
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip(), key
        alt = normalized.get(_norm_key(key))
        if alt is not None and str(alt[1]).strip():
            return str(alt[1]).strip(), str(alt[0])
    return "", ""


def clean_spaces(value: str) -> str:
    text = str(value or "").replace("\ufeff", "").replace("\u200b", "")
    text = text.replace("　", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip(" \t\r\n-|｜:：／/[]【】")


def _format_ascii_brand(value: str) -> str:
    text = clean_spaces(value)
    if re.fullmatch(r"[A-Za-z0-9&.'() -]{2,48}", text) and text == text.lower():
        return " ".join(part.capitalize() if part.isalpha() else part for part in text.split(" "))
    return text


def clean_domain(value: str) -> str:
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


def extract_domain_from_row(row: dict[str, str]) -> str:
    for field in DOMAIN_FIELDS:
        domain = clean_domain(_pick(row, [field]))
        if domain:
            return domain
    return ""


def domain_display_name(domain: str) -> str:
    domain = clean_domain(domain)
    if not domain:
        return ""
    parts = [part for part in domain.split(".") if part]
    if len(parts) >= 3 and parts[-1] == "jp" and parts[-2] in JP_PUBLIC_SUFFIX_SECOND_LEVELS:
        label = parts[-3]
    elif len(parts) >= 2:
        label = parts[-2]
    else:
        label = parts[0]

    label = re.sub(r"[^a-z0-9_-]+", " ", label, flags=re.IGNORECASE)
    label = label.replace("_", "-")
    if "-" not in label and label.endswith("fukuoka") and len(label) > len("fukuoka") + 3:
        label = f"{label[:-7]}-fukuoka"
    words = [word for word in re.split(r"[-\s]+", label) if word]
    if not words:
        return ""
    return " ".join(word.upper() if len(word) <= 3 and word.isupper() else word.capitalize() for word in words)


def _strip_known_noise(value: str) -> str:
    text = clean_spaces(value)
    for pattern in NOISE_STRIP_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    return clean_spaces(text)


def _strip_title_prefix(value: str) -> str:
    text = clean_spaces(value)
    changed = True
    while changed:
        changed = False
        for pattern in TITLE_PREFIX_PATTERNS:
            updated = re.sub(pattern, "", text, flags=re.IGNORECASE)
            updated = clean_spaces(updated)
            if updated and updated != text:
                text = updated
                changed = True
    return text


def _is_generic(value: str) -> bool:
    compact = re.sub(r"\s+", " ", str(value or "").strip()).lower()
    jp_compact = re.sub(r"\s+", "", str(value or "").strip())
    if compact in GENERIC_TITLES or jp_compact in GENERIC_TITLES:
        return True
    if compact.startswith("music room") and len(compact) <= 16:
        return True
    return False


def _warnings_for(value: str) -> list[str]:
    warnings: list[str] = []
    text = clean_spaces(value)
    lowered = text.lower()
    compact = re.sub(r"\s+", "", text)
    if not text:
        return ["low_confidence", "human_review_needed"]
    if len(text) > 28:
        warnings.append("title_too_long")
    if _is_generic(text):
        warnings.append("generic_title")
    if any(token.lower() in lowered for token in PROMOTIONAL_TOKENS):
        warnings.append("promotional_title")
    comma_count = text.count("、") + text.count(",") + text.count("・")
    category_hits = sum(1 for token in CATEGORY_TOKENS if token.lower() in lowered)
    if category_hits >= 3 and comma_count >= 2:
        warnings.append("category_list")
    if compact in {re.sub(r"\s+", "", token) for token in CATEGORY_TOKENS}:
        warnings.append("category_list")
    if any(token in text for token in LOCATION_TOKENS) and len(text) <= 18:
        warnings.append("address_like")
    if re.search(r"\d+\.\d|\d+件|\d+\s*reviews?", lowered):
        warnings.append("review_text_like")
    if re.search(r"\d{2,4}[-ー]\d{2,4}", text):
        warnings.append("address_like")
    return list(dict.fromkeys(warnings))


def _score_candidate(value: str, source: str, *, quoted: bool = False) -> tuple[int, str, list[str]]:
    text = _format_ascii_brand(_strip_known_noise(value))
    warnings = _warnings_for(text)
    if not text:
        return 0, "", warnings

    source_key = source.lower()
    if source_key == "explicit":
        score = 92
    elif source_key == "og_site_name":
        score = 86
    elif source_key == "title_cleaned":
        score = 72
    else:
        score = 70

    if quoted:
        score += 12
    if any(suffix.lower() in text.lower() for suffix in BUSINESS_SUFFIXES):
        score += 8
    if re.search(r"[A-Za-z]{3,}\s+[A-Za-z0-9]{2,}", text):
        score += 8
    if re.fullmatch(r"[A-Za-z0-9&.' -]{3,32}", text) and len(text.split()) <= 4:
        score += 4
    if source_key == "title_cleaned" and TITLE_SEPARATORS_RE.search(text):
        score -= 20
    if source_key == "title_cleaned" and _strip_title_prefix(text) != text:
        score -= 18

    penalty_map = {
        "generic_title": 90,
        "promotional_title": 45,
        "category_list": 75,
        "title_too_long": 25,
        "address_like": 30,
        "review_text_like": 35,
    }
    for warning in warnings:
        if warning == "address_like" and quoted and any(suffix.lower() in text.lower() for suffix in BUSINESS_SUFFIXES):
            continue
        score -= penalty_map.get(warning, 0)

    if source_key == "title_cleaned" and "promotional_title" in warnings:
        score -= 10
    if source_key == "explicit" and any(w in warnings for w in ("generic_title", "promotional_title", "category_list")):
        score -= 45

    score = max(0, min(100, score))
    confidence = "high" if score >= 80 else "medium" if score >= 55 else "low"
    return score, text, warnings


def _title_variants(title: str) -> list[tuple[str, bool]]:
    text = _strip_known_noise(title)
    variants: list[tuple[str, bool]] = []
    if not text:
        return variants

    for pattern in [r"「([^」]{2,48})」", r"『([^』]{2,48})』", r"【([^】]{2,48})】", r"\[([^\]]{2,48})\]"]:
        for match in re.finditer(pattern, text):
            variants.append((clean_spaces(match.group(1)), True))

    parts = [clean_spaces(part) for part in TITLE_SEPARATORS_RE.split(text) if clean_spaces(part)]
    variants.extend((part, False) for part in parts)
    variants.append((text, False))

    prefixed = _strip_title_prefix(text)
    if prefixed and prefixed != text:
        variants.append((prefixed, False))
    for part in parts:
        prefixed_part = _strip_title_prefix(part)
        if prefixed_part and prefixed_part != part:
            variants.append((prefixed_part, False))

    for pattern in [
        r"(?:なら|ならば)\s*([^|｜│／/:：]+)$",
        r"(?:の|で)(?:音楽スクール|音楽教室|ヨガ|ピラティス|フィットネス|整体|整骨院|鍼灸|美容鍼灸|サロン)(?:なら|は)?\s*([^|｜│／/:：]+)",
        r"([A-Z][A-Za-z0-9&.' -]{2,}(?:Studio|Salon|Meeting|Haku|Casablanca|MII|Room)?[A-Za-z0-9&.' -]*)",
    ]:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            variants.append((clean_spaces(match.group(1)), False))

    deduped: list[tuple[str, bool]] = []
    seen: set[str] = set()
    for value, quoted in variants:
        cleaned = clean_spaces(value)
        extra_values = [cleaned]
        paren_base = clean_spaces(re.sub(r"\s*[（(].*?[）)]\s*$", "", cleaned))
        if paren_base and paren_base != cleaned:
            extra_values.append(paren_base)
        for candidate in extra_values:
            key = candidate.lower()
            if candidate and key not in seen:
                deduped.append((candidate, quoted))
                seen.add(key)
    return deduped


def _existing_name_is_title_replay(row: dict[str, str], source: str, value: str) -> bool:
    name_source = _pick(row, ["name_source"]).lower()
    if name_source in TITLE_DERIVED_SOURCES or name_source in DOMAIN_SOURCES:
        return True
    warning = _pick(row, ["name_warning"]).lower()
    if any(token in warning for token in ["generic_title", "promotional_title", "category_list", "domain_fallback"]):
        return True
    if source == "display_name" and set(_warnings_for(value)) & {
        "generic_title",
        "promotional_title",
        "category_list",
        "title_too_long",
        "review_text_like",
    }:
        return True
    return False


def clean_display_name(row: dict[str, str], domain: str = "") -> NameResult:
    domain = clean_domain(domain) or extract_domain_from_row(row)
    original_display_name = _pick(row, ["original_display_name", "display_name", "business_name", "salon_name", "brand_name", "company_name", "店名"])
    original_title = _pick(row, TITLE_FIELDS)
    candidates: list[tuple[str, str, bool]] = []

    for field in EXPLICIT_NAME_FIELDS:
        value, source = _pick_with_source(row, [field])
        if not value:
            continue
        if _existing_name_is_title_replay(row, source, value):
            candidates.extend((variant, "title_cleaned", quoted) for variant, quoted in _title_variants(value))
            continue
        candidates.append((value, "explicit", False))

    for field in SITE_NAME_FIELDS:
        value, _source = _pick_with_source(row, [field])
        if value:
            candidates.append((value, "og_site_name", False))

    for field in TITLE_FIELDS:
        value, _source = _pick_with_source(row, [field])
        if value:
            candidates.extend((variant, "title_cleaned", quoted) for variant, quoted in _title_variants(value))

    best_value = ""
    best_source = ""
    best_score = -1
    best_confidence = "low"
    best_warnings: list[str] = []
    for value, source, quoted in candidates:
        score, cleaned, warnings = _score_candidate(value, source, quoted=quoted)
        if score > best_score:
            best_value = cleaned
            best_source = source
            best_score = score
            best_confidence = "high" if score >= 80 else "medium" if score >= 55 else "low"
            best_warnings = warnings

    domain_fallback = domain_display_name(domain)
    title_single_ascii_fragment = (
        best_source == "title_cleaned"
        and bool(re.fullmatch(r"[A-Za-z]{3,24}", best_value or ""))
        and len(domain_fallback.split()) >= 2
    )
    fallback_preferred = (
        best_score < 55
        or title_single_ascii_fragment
        or (
            best_source == "title_cleaned"
            and best_score < 80
            and bool(set(best_warnings) & {"generic_title", "promotional_title", "category_list", "title_too_long"})
        )
    )
    if fallback_preferred:
        fallback = domain_fallback
        if fallback:
            confidence = "medium" if len(fallback) >= 5 else "low"
            warnings = ["domain_fallback"]
            if confidence == "low":
                warnings.append("human_review_needed")
            return NameResult(
                display_name=fallback,
                name_confidence=confidence,
                name_source="domain",
                name_warning=";".join(warnings),
                original_display_name=original_display_name,
                original_title=original_title,
                score=55 if confidence == "medium" else 40,
            )
        return NameResult(
            display_name=best_value or original_display_name,
            name_confidence="low",
            name_source=best_source or "manual_review",
            name_warning="low_confidence;human_review_needed",
            original_display_name=original_display_name,
            original_title=original_title,
            score=max(best_score, 0),
        )

    warnings = list(dict.fromkeys(best_warnings))
    if best_confidence == "low" and "low_confidence" not in warnings:
        warnings.append("low_confidence")
    return NameResult(
        display_name=best_value,
        name_confidence=best_confidence,
        name_source=best_source,
        name_warning=";".join(warnings),
        original_display_name=original_display_name,
        original_title=original_title,
        score=best_score,
    )


def clean_row_names(row: dict[str, str], domain: str = "") -> tuple[dict[str, str], NameResult]:
    result = clean_display_name(row, domain)
    cleaned = dict(row)
    display_name = result.display_name
    for field in OUTPUT_NAME_FIELDS:
        cleaned[field] = display_name
    cleaned["name_confidence"] = result.name_confidence
    cleaned["name_source"] = result.name_source
    cleaned["name_warning"] = result.name_warning
    cleaned["original_display_name"] = result.original_display_name
    cleaned["original_title"] = result.original_title
    return cleaned, result


def suggested_action(result: NameResult) -> str:
    warnings = set(filter(None, result.name_warning.split(";")))
    if result.name_confidence == "low" or "human_review_needed" in warnings:
        return "human_review"
    if clean_spaces(result.display_name) != clean_spaces(result.original_display_name):
        return "use_cleaned"
    return "keep"


def build_review_row(row: dict[str, str], result: NameResult) -> dict[str, str]:
    return {
        "lead_id": _pick(row, ["lead_id", "id"]),
        "domain": clean_domain(_pick(row, ["domain", "original__domain"])) or extract_domain_from_row(row),
        "original_display_name": result.original_display_name,
        "cleaned_display_name": result.display_name,
        "name_confidence": result.name_confidence,
        "name_source": result.name_source,
        "name_warning": result.name_warning,
        "title": result.original_title,
        "website": _pick(row, ["website", "url", "reference_url", "url(旧)"]),
        "contact_url": _pick(row, ["contact_url", "contact_page", "original__contact_url", "original__form_url"]),
        "tier": _pick(row, ["lead_tier", "tier"]),
        "suggested_action": suggested_action(result),
    }


def _read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader), list(reader.fieldnames or [])


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_summary(path: Path, review_rows: list[dict[str, str]]) -> None:
    confidence_counts = Counter(row["name_confidence"] for row in review_rows)
    action_counts = Counter(row["suggested_action"] for row in review_rows)
    warning_counts: Counter[str] = Counter()
    examples: list[dict[str, str]] = []
    for row in review_rows:
        for warning in filter(None, row["name_warning"].split(";")):
            warning_counts[warning] += 1
        if row["suggested_action"] != "keep" and len(examples) < 12:
            examples.append(row)

    lines = [
        "# Display Name Quality Review",
        "",
        f"- total rows checked: {len(review_rows)}",
        f"- high confidence: {confidence_counts.get('high', 0)}",
        f"- medium confidence: {confidence_counts.get('medium', 0)}",
        f"- low confidence: {confidence_counts.get('low', 0)}",
        f"- rows needing human review: {action_counts.get('human_review', 0)}",
        "",
        "## Suggested Actions",
        "",
        f"- keep: {action_counts.get('keep', 0)}",
        f"- use_cleaned: {action_counts.get('use_cleaned', 0)}",
        f"- human_review: {action_counts.get('human_review', 0)}",
        "",
        "## Noisy Patterns Found",
        "",
    ]
    if warning_counts:
        lines.extend(f"- {key}: {value}" for key, value in warning_counts.most_common())
    else:
        lines.append("- none")
    lines.extend(["", "## Before/After Examples", ""])
    if examples:
        for row in examples:
            lines.extend(
                [
                    f"- domain: {row['domain']}",
                    f"  - before: {row['original_display_name']}",
                    f"  - after: {row['cleaned_display_name']}",
                    f"  - confidence: {row['name_confidence']}",
                    f"  - warning: {row['name_warning']}",
                    f"  - action: {row['suggested_action']}",
                ]
            )
    else:
        lines.append("- none")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_review(input_path: Path, review_output: Path, summary_output: Path, cleaned_output: Path | None = None) -> tuple[int, Counter[str]]:
    rows, fieldnames = _read_csv(input_path)
    review_rows: list[dict[str, str]] = []
    cleaned_rows: list[dict[str, str]] = []
    action_counts: Counter[str] = Counter()
    for row in rows:
        cleaned, result = clean_row_names(row)
        review_row = build_review_row(row, result)
        review_rows.append(review_row)
        cleaned_rows.append(cleaned)
        action_counts[review_row["suggested_action"]] += 1

    _write_csv(review_output, REVIEW_FIELDS, review_rows)
    write_summary(summary_output, review_rows)
    if cleaned_output:
        output_fields = list(fieldnames)
        for field in ["original_display_name", "original_title"]:
            if field not in output_fields:
                output_fields.append(field)
        for field in OUTPUT_NAME_FIELDS + ["name_confidence", "name_source", "name_warning"]:
            if field not in output_fields:
                output_fields.append(field)
        _write_csv(cleaned_output, output_fields, cleaned_rows)
    return len(rows), action_counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Review and clean local display_name values in a CSV.")
    parser.add_argument("--input", required=True, help="Input local CSV.")
    parser.add_argument(
        "--review-output",
        default=str(Path(__file__).resolve().parents[1] / "web_app" / "output" / "display_name_quality_review.csv"),
        help="Output display-name quality review CSV.",
    )
    parser.add_argument(
        "--summary-output",
        default=str(Path(__file__).resolve().parents[1] / "web_app" / "output" / "display_name_quality_review.summary.md"),
        help="Output markdown summary.",
    )
    parser.add_argument("--cleaned-output", default="", help="Optional full CSV with cleaned name fields applied.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    count, actions = run_review(
        Path(args.input),
        Path(args.review_output),
        Path(args.summary_output),
        Path(args.cleaned_output) if args.cleaned_output else None,
    )
    print(f"rows_checked={count}")
    for key in ["keep", "use_cleaned", "human_review"]:
        print(f"{key}={actions.get(key, 0)}")
    print(f"review_output={args.review_output}")
    print(f"summary_output={args.summary_output}")
    if args.cleaned_output:
        print(f"cleaned_output={args.cleaned_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
