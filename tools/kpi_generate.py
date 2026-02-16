#!/usr/bin/env python3
"""
KPI governance generator for lead-finder.

This module reads governance files (CEO.md / OPS.md / KPI.md), evaluates a lead CSV
with KPI.md-aligned formulas, and writes:
  - KPI.json (runtime source of truth)
  - KPI_REPORT.md (concise executive report)

CLI:
  python -m tools.kpi_generate --input web_app/output/merge_fukuoka_all_queries.csv --out KPI.json --report KPI_REPORT.md
  python -m tools.kpi_generate --input web_app/output/merge_fukuoka_all_queries.csv --slice 200
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


JST = ZoneInfo("Asia/Tokyo")
ROOT = Path(__file__).resolve().parents[1]

DEFAULT_INPUT = ROOT / "web_app" / "output" / "merge_fukuoka_all_queries.csv"
DEFAULT_OUT = ROOT / "KPI.json"
DEFAULT_REPORT = ROOT / "KPI_REPORT.md"


@dataclass(frozen=True)
class PhaseThresholds:
    solo_rate_min: float = 0.60
    corporate_rate_max: float = 0.25
    unknown_rate_max: float = 0.20
    bad_domain_mix_max: float = 0.02
    city_missing_rate_max: float = 0.10


@dataclass(frozen=True)
class ParsedThresholds:
    phase1: PhaseThresholds
    phase2_solo_rate_min: float
    phase2_total_positive_leads_min: int
    phase2_bad_domain_mix_max: float
    phase3_prepared_rate_min: float
    phase3_safe_submit_rate_min: float
    phase3_complaint_or_block_rate: float


@dataclass
class LeadEval:
    rank_key_score: float
    domain: str
    shop_name: str
    label: str
    label_reason: str
    is_noise: bool
    noise_reason: str
    score: float
    is_positive: bool
    city_detected: bool
    classification: str
    url: str


SCORE_COLS = (
    "リードスコア",
    "lead_score",
    "score",
    "スコア",
)

URL_COLS = (
    "最終URL",
    "URL",
    "url",
    "final_url",
    "contact_url",
    "original_url",
)

CITY_COLS = (
    "市区町村",
    "city",
    "area",
    "region_city",
    "住所",
    "所在地",
)

PREF_COLS = (
    "都道府県",
    "prefecture",
    "地方",
    "region",
)

LABEL_COLS = (
    "営業優先度",
    "sales_label",
    "label",
    "lead_quality_tag",
)

LABEL_REASON_COLS = (
    "営業ラベル理由",
    "label_reason",
    "reason",
    "フィルタ理由",
    "コメント",
)

CLASS_COLS = (
    "個人度分類",
    "solo_classification",
    "classification",
    "size_class",
)

SHOP_NAME_COLS = (
    "店舗名",
    "表示名",
    "store_name",
    "shop_name",
    "name",
)

QUERY_COLS = (
    "検索クエリ",
    "search_query",
    "query",
)

TEXT_FALLBACK_COLS = (
    "検索クエリ",
    "コメント",
    "営業ラベル理由",
    "フィルタ理由",
    "個人度理由",
    "個人度根拠",
    "source_business_types",
)

CITY_REGEX = re.compile(r"([^\s,，]{1,16}(?:市|区|町|村))")

# KPI.md noise definition categories:
# adult, global media/news/weather/sports, market/finance portals,
# generic ranking/review aggregators, large corporate chains, government/association.
NOISE_DOMAIN_MARKERS: dict[str, tuple[str, ...]] = {
    "adult": (
        "pornhub.",
        "xvideos.",
        "xhamster.",
        "xnxx.",
        "redtube.",
        "youporn.",
    ),
    "weather": (
        "weather.com",
        "accuweather.com",
        "weathernews.",
        "tenki.jp",
    ),
    "sports": (
        "mlb.com",
        "espn.com",
        "sports.yahoo.",
        "nfl.com",
        "nba.com",
    ),
    "market_finance": (
        "marketwatch.com",
        "bloomberg.com",
        "investing.com",
        "tradingview.com",
        "nasdaq.com",
        "nikkei.com",
    ),
    "global_media_news": (
        "cnn.com",
        "bbc.",
        "nytimes.com",
        "reuters.com",
        "apnews.com",
        "forbes.com",
    ),
    "aggregator_portal": (
        "hotpepper.jp",
        "ekiten.jp",
        "rakuten.co.jp",
        "retty.me",
        "jalan.net",
        "beauty.hotpepper.jp",
        "mybest.com",
    ),
    "large_chain": (
        "aeon.",
        "mcdonalds.",
        "starbucks.",
        "docomo.",
        "softbank.",
    ),
    "gov_association": (
        ".go.jp",
        ".lg.jp",
        "city.",
        "pref.",
        "association",
        "kyokai",
    ),
}

NOISE_PATH_MARKERS: tuple[str, ...] = (
    "/ranking",
    "/rankings",
    "/matome",
    "/osusume",
    "/comparison",
    "/compare",
    "/review",
    "/reviews",
    "/article",
    "/column",
)

GLOBAL_MEDIA_NOISE_DOMAINS: tuple[str, ...] = (
    "forbes.com",
    "cnn.com",
    "bbc.com",
    "reuters.com",
    "apnews.com",
)

LOCAL_BUSINESS_MARKERS: tuple[str, ...] = (
    "サロン",
    "整体",
    "美容",
    "カウンセリング",
    "セラピー",
    "個人",
    "予約",
    "福岡",
    "北九州",
    "久留米",
    "市",
    "区",
    "町",
    "村",
    "fukuoka",
    "salon",
    "therapy",
    "counseling",
    "private",
    "owner",
    "clinic",
)

CLASS_SOLO_MARKERS = ("solo", "個人", "一人", "1人", "small", "小規模")
CLASS_CORPORATE_MARKERS = ("corporate", "法人", "株式会社", "有限会社", "group", "グループ")
UNKNOWN_TO_CORPORATE_STRONG_MARKERS = (
    "公益社団法人",
    "一般社団法人",
    "公益財団法人",
    "一般財団法人",
    "社会福祉法人",
    "医療法人",
    "学校法人",
    "宗教法人",
    "特定非営利活動法人",
    "npo法人",
    "行政",
    "自治体",
    "協同組合",
    "連合会",
    "事業団",
    "公社",
    "公団",
    "NPO法人",
    "商工会",
)

UNKNOWN_TO_CORPORATE_DOMAIN_MARKERS = (
    "login_subdomain",
    "accounts_subdomain",
    "select-type.com",
    "aliexpress.com",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate KPI.json and KPI_REPORT.md from lead CSV.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Input lead CSV path")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="Output KPI.json path")
    parser.add_argument("--report", default=str(DEFAULT_REPORT), help="Output KPI report markdown path")
    parser.add_argument("--slice", type=int, default=None, help="Evaluate only first N rows (safe slice)")
    return parser.parse_args()


def read_text_safe(path: Path) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp932", "shift_jis"):
        try:
            return path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="replace")


def load_governance(root: Path) -> dict[str, str]:
    docs = {}
    for name in ("CEO.md", "OPS.md", "KPI.md"):
        p = root / name
        docs[name] = read_text_safe(p) if p.exists() else ""
    return docs


def parse_kpi_thresholds(kpi_md_text: str) -> ParsedThresholds:
    def _find_float(key: str, default: float) -> float:
        pattern = re.compile(rf"{re.escape(key)}[^\d]*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
        m = pattern.search(kpi_md_text)
        return float(m.group(1)) if m else default

    def _find_int(key: str, default: int) -> int:
        pattern = re.compile(rf"{re.escape(key)}[^\d]*([0-9]+)", re.IGNORECASE)
        m = pattern.search(kpi_md_text)
        return int(m.group(1)) if m else default

    phase1 = PhaseThresholds(
        solo_rate_min=_find_float("solo_rate", 0.60),
        corporate_rate_max=_find_float("corporate_rate", 0.25),
        unknown_rate_max=_find_float("unknown_rate", 0.20),
        bad_domain_mix_max=_find_float("bad_domain_mix", 0.02),
        city_missing_rate_max=_find_float("city_missing_rate", 0.10),
    )
    # For governance alignment; not all are currently computable from lead CSV alone.
    return ParsedThresholds(
        phase1=phase1,
        phase2_solo_rate_min=0.55 if "0.55" in kpi_md_text else _find_float("solo_rate", 0.55),
        phase2_total_positive_leads_min=_find_int("total_positive_leads", 300),
        phase2_bad_domain_mix_max=0.03 if "0.03" in kpi_md_text else _find_float("bad_domain_mix", 0.03),
        phase3_prepared_rate_min=_find_float("prepared_rate", 0.30),
        phase3_safe_submit_rate_min=_find_float("safe_submit_rate", 0.15),
        phase3_complaint_or_block_rate=_find_float("complaint_or_block_rate", 0.0),
    )


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    last_error: Exception | None = None
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except Exception as exc:  # pragma: no cover
            last_error = exc
    raise RuntimeError(f"Unable to read CSV with supported encodings: {last_error}")


def _first_value(row: dict[str, Any], candidates: tuple[str, ...]) -> str:
    for key in candidates:
        if key in row:
            v = row.get(key)
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
    return ""


def _to_float(s: str) -> float:
    if not s:
        return 0.0
    try:
        return float(str(s).replace(",", "").strip())
    except Exception:
        return 0.0


def _safe_ratio(n: int, d: int) -> float:
    if d <= 0:
        return 0.0
    return round(float(n) / float(d), 6)


def extract_domain_and_url(row: dict[str, str]) -> tuple[str, str]:
    raw_url = _first_value(row, URL_COLS)
    if not raw_url:
        return "", ""
    candidate = raw_url if "://" in raw_url else f"https://{raw_url}"
    parsed = urlparse(candidate)
    domain = (parsed.netloc or "").lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]
    if not parsed.scheme and parsed.path:
        # urlparse fallback when no scheme and no netloc.
        domain = parsed.path.lower().strip("/")
    return domain, raw_url


def classify_noise(domain: str, row: dict[str, str]) -> tuple[bool, str]:
    if not domain:
        return False, ""
    path = ""
    raw_url = _first_value(row, URL_COLS)
    if raw_url:
        parsed = urlparse(raw_url if "://" in raw_url else f"https://{raw_url}")
        path = (parsed.path or "").lower()

    lower_domain = domain.lower()
    for category, markers in NOISE_DOMAIN_MARKERS.items():
        for marker in markers:
            if marker in lower_domain:
                if category == "global_media_news" and _looks_local_business_pattern(row):
                    continue
                return True, f"domain:{category}:{marker}"

    for marker in NOISE_PATH_MARKERS:
        if marker in path:
            return True, f"path:aggregator:{marker}"

    return False, ""


def _looks_local_business_pattern(row: dict[str, str]) -> bool:
    parts: list[str] = []
    for key in (*QUERY_COLS, *TEXT_FALLBACK_COLS, *SHOP_NAME_COLS, *CITY_COLS):
        if key in row:
            v = str(row.get(key, "")).strip()
            if v:
                parts.append(v.lower())
    blob = " ".join(parts)
    return any(marker.lower() in blob for marker in LOCAL_BUSINESS_MARKERS)


def load_ops_auto_excluded_domains(filters_path: Path) -> set[str]:
    if not filters_path.exists():
        return set()
    text = read_text_safe(filters_path)
    pattern = re.compile(
        r"# OPS_AUTO_BLOCKLIST_START\s*OPS_AUTO_EXCLUDED_DOMAINS\s*=\s*\{(.*?)\}\s*# OPS_AUTO_BLOCKLIST_END",
        re.DOTALL,
    )
    m = pattern.search(text)
    if not m:
        return set()
    body = m.group(1)
    found = re.findall(r"'([^']+)'", body)
    normalized: set[str] = set()
    for d in found:
        clean = d.strip().lower()
        if clean.startswith("www."):
            clean = clean[4:]
        if clean and "." in clean:
            normalized.add(clean)
    return normalized


def detect_city(row: dict[str, str]) -> tuple[bool, str]:
    city = _first_value(row, CITY_COLS)
    if city:
        return True, city

    pref = _first_value(row, PREF_COLS)
    if pref and any(x in pref for x in ("都", "道", "府", "県")):
        # Prefecture only is weaker than city but still location signal.
        return True, pref

    texts: list[str] = []
    for col in TEXT_FALLBACK_COLS:
        if col in row:
            v = str(row.get(col, "")).strip()
            if v:
                texts.append(v)
    blob = " ".join(texts)
    m = CITY_REGEX.search(blob)
    if m:
        return True, m.group(1)
    return False, ""


def _unknown_to_corporate_keyword(row: dict[str, str]) -> str:
    # For safety, only use strong legal-entity signals in title/name/reason-like fields.
    parts: list[str] = []
    for key in (
        "title",
        "shop_name",
        "visible_text",
        "reasons",
        "店舗名",
        "表示名",
        "store_name",
        "shop_name",
        "name",
        "営業ラベル理由",
        "コメント",
        "フィルタ理由",
    ):
        if key in row:
            v = str(row.get(key, "")).strip()
            if v:
                parts.append(v.lower())
    blob = " ".join(parts)
    for marker in UNKNOWN_TO_CORPORATE_STRONG_MARKERS:
        if marker.lower() in blob:
            return marker
    return ""


def _unknown_to_corporate_domain_keyword(domain: str) -> str:
    d = (domain or "").lower()
    if d.startswith("login."):
        return "login_subdomain"
    if d.startswith("accounts."):
        return "accounts_subdomain"
    if d == "select-type.com" or d.endswith(".select-type.com"):
        return "select-type.com"
    if d == "aliexpress.com" or d.endswith(".aliexpress.com"):
        return "aliexpress.com"
    return ""


def classify_size_with_reason(row: dict[str, str]) -> tuple[str, str]:
    raw = _first_value(row, CLASS_COLS).lower()
    if raw:
        if any(m in raw for m in CLASS_SOLO_MARKERS):
            if "small" in raw or "小規模" in raw:
                return "small", ""
            return "solo", ""
        if any(m in raw for m in CLASS_CORPORATE_MARKERS):
            return "corporate", ""
        if "unknown" in raw or "不明" in raw:
            pass

    blob_parts = []
    for key in ("個人度理由", "個人度根拠", "コメント", "フィルタ理由"):
        if key in row and row[key]:
            blob_parts.append(str(row[key]).lower())
    blob = " ".join(blob_parts)
    if blob:
        if any(m in blob for m in ("個人", "一人", "small", "小規模")):
            return "solo", ""
        if any(m in blob for m in ("法人", "株式会社", "corporate", "chain", "グループ")):
            return "corporate", ""

    kw = _unknown_to_corporate_keyword(row)
    if kw:
        return "corporate", f"unknown_to_corporate:{kw}"
    return "unknown", ""


def classify_size(row: dict[str, str]) -> str:
    classification, _ = classify_size_with_reason(row)
    return classification


def normalize_label(raw_label: str) -> str:
    s = (raw_label or "").strip().upper()
    if s in {"○", "O", "GOOD"}:
        return "○"
    if s in {"△", "DELTA", "BORDERLINE"}:
        return "△"
    if s in {"×", "X", "EXCLUDE_CLEAR", "EXCLUDE"}:
        return "×"
    return ""


def infer_label(is_noise: bool, score: float, city_detected: bool, classification: str) -> tuple[str, str]:
    if is_noise:
        return "×", "inferred:noise"
    if score > 0 and city_detected and classification in {"solo", "small"}:
        return "○", "inferred:positive_city_solo_small"
    if score > 0:
        return "△", "inferred:positive_not_strong_target"
    return "×", "inferred:non_positive_score"


def current_jst_timestamp() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def run_id_from_timestamp(ts: str) -> str:
    compact = ts.replace("-", "").replace(" ", "_").replace(":", "")
    return f"kpi_{compact}"


def get_git_commit(root: Path) -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=root,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or "unknown"
    except Exception:
        return "unknown"


def build_config_hash(governance: dict[str, str], thresholds: ParsedThresholds) -> str:
    governance_hashes = {
        name: hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()
        for name, content in governance.items()
    }
    payload = {
        "thresholds": {
            "phase1": thresholds.phase1.__dict__,
            "phase2": {
                "solo_rate_min": thresholds.phase2_solo_rate_min,
                "total_positive_leads_min": thresholds.phase2_total_positive_leads_min,
                "bad_domain_mix_max": thresholds.phase2_bad_domain_mix_max,
            },
            "phase3": {
                "prepared_rate_min": thresholds.phase3_prepared_rate_min,
                "safe_submit_rate_min": thresholds.phase3_safe_submit_rate_min,
                "complaint_or_block_rate": thresholds.phase3_complaint_or_block_rate,
            },
        },
        "noise_domain_markers": NOISE_DOMAIN_MARKERS,
        "noise_path_markers": NOISE_PATH_MARKERS,
        "columns": {
            "score": SCORE_COLS,
            "url": URL_COLS,
            "city": CITY_COLS,
            "prefecture": PREF_COLS,
            "label": LABEL_COLS,
            "classification": CLASS_COLS,
        },
        "governance_hashes": governance_hashes,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def evaluate_rows(
    rows: list[dict[str, str]],
    notes: list[str],
    ops_auto_domains: set[str] | None = None,
) -> tuple[list[LeadEval], Counter[str], Counter[str], list[str], list[dict[str, str]]]:
    evaluated: list[LeadEval] = []
    bad_domains_counter: Counter[str] = Counter()
    noise_reasons_counter: Counter[str] = Counter()
    missing_city_examples: list[str] = []
    unknown_examples: list[dict[str, str]] = []
    ops_auto_domains = ops_auto_domains or set()

    for row in rows:
        domain, raw_url = extract_domain_and_url(row)
        score = _to_float(_first_value(row, SCORE_COLS))
        if not _first_value(row, SCORE_COLS):
            # Fallback to score if lead_score is unavailable.
            score = _to_float(_first_value(row, ("スコア", "score")))

        city_detected, city_value = detect_city(row)
        classification, class_reason = classify_size_with_reason(row)
        if classification == "unknown":
            domain_kw = _unknown_to_corporate_domain_keyword(domain)
            if domain_kw:
                classification = "corporate"
                class_reason = (
                    f"{class_reason}; unknown_to_corporate:{domain_kw}"
                    if class_reason
                    else f"unknown_to_corporate:{domain_kw}"
                )
        if (
            domain
            and any(domain == d or domain.endswith("." + d) for d in ops_auto_domains)
            and not (
                any(domain == d or domain.endswith("." + d) for d in GLOBAL_MEDIA_NOISE_DOMAINS)
                and _looks_local_business_pattern(row)
            )
        ):
            is_noise, noise_reason = True, "domain:ops_auto"
        else:
            is_noise, noise_reason = classify_noise(domain, row)
        if is_noise and domain:
            bad_domains_counter[domain] += 1
            noise_reasons_counter[noise_reason] += 1

        raw_label = _first_value(row, LABEL_COLS)
        label = normalize_label(raw_label)
        label_reason = _first_value(row, LABEL_REASON_COLS)
        if class_reason:
            label_reason = f"{label_reason}; {class_reason}" if label_reason else class_reason

        if not label:
            inferred, inferred_reason = infer_label(is_noise, score, city_detected, classification)
            label = inferred
            if not label_reason:
                label_reason = inferred_reason

        is_positive = (score > 0.0) and (not is_noise)
        if is_positive and classification == "unknown" and len(unknown_examples) < 10:
            marker = _unknown_to_corporate_keyword(row) or _unknown_to_corporate_domain_keyword(domain)
            unknown_examples.append(
                {
                    "domain": domain,
                    "shop_name": _first_value(row, SHOP_NAME_COLS),
                    "marker_hit": "true" if marker else "false",
                    "marker": marker or "",
                }
            )
        if is_positive and (not city_detected):
            example = raw_url or domain or _first_value(row, SHOP_NAME_COLS)
            if example and len(missing_city_examples) < 20:
                missing_city_examples.append(example)

        evaluated.append(
            LeadEval(
                rank_key_score=score,
                domain=domain,
                shop_name=_first_value(row, SHOP_NAME_COLS),
                label=label,
                label_reason=label_reason,
                is_noise=is_noise,
                noise_reason=noise_reason,
                score=score,
                is_positive=is_positive,
                city_detected=city_detected,
                classification=classification,
                url=raw_url,
            )
        )

        if not domain:
            notes.append("missing_domain_from_url_row_detected")

        if not _first_value(row, SCORE_COLS) and not _first_value(row, ("スコア", "score")):
            notes.append("missing_score_column_or_value_detected")

    return evaluated, bad_domains_counter, noise_reasons_counter, missing_city_examples, unknown_examples


def phase_status(rates: dict[str, float], thresholds: PhaseThresholds) -> tuple[bool, list[str]]:
    blocking: list[str] = []
    if rates["solo_rate"] < thresholds.solo_rate_min:
        blocking.append("solo_rate")
    if rates["corporate_rate"] > thresholds.corporate_rate_max:
        blocking.append("corporate_rate")
    if rates["unknown_rate"] > thresholds.unknown_rate_max:
        blocking.append("unknown_rate")
    if rates["bad_domain_mix"] > thresholds.bad_domain_mix_max:
        blocking.append("bad_domain_mix")
    if rates["city_missing_rate"] > thresholds.city_missing_rate_max:
        blocking.append("city_missing_rate")
    return len(blocking) == 0, blocking


def _is_forced_noise_for_top50(row: LeadEval) -> bool:
    domain = (row.domain or "").lower()
    if domain.endswith(".go.jp") or domain.endswith(".lg.jp"):
        return True

    reason_blob = f"{row.label_reason} {row.noise_reason}".lower()
    if "domain:gov_association" in reason_blob:
        return True
    if "path:aggregator" in reason_blob:
        return True
    return False


def compute_kpi_payload(
    rows: list[dict[str, str]],
    input_csv: str,
    slice_value: int | None,
    governance: dict[str, str],
    thresholds: ParsedThresholds,
) -> dict[str, Any]:
    notes: list[str] = []

    # Deduplicate identical notes while preserving order.
    def _dedupe_notes(src: list[str]) -> list[str]:
        seen = set()
        out = []
        for n in src:
            if n not in seen:
                seen.add(n)
                out.append(n)
        return out

    ops_auto_domains = load_ops_auto_excluded_domains(ROOT / "src" / "filters.py")
    evaluated, bad_domains_counter, noise_reasons_counter, missing_city_examples, unknown_examples = evaluate_rows(
        rows,
        notes,
        ops_auto_domains=ops_auto_domains,
    )
    total_leads = len(evaluated)
    noise_leads = sum(1 for r in evaluated if r.is_noise)
    positive_leads = sum(1 for r in evaluated if r.is_positive)
    positive_quality_leads = sum(1 for r in evaluated if r.is_positive and r.city_detected)

    solo_count = sum(1 for r in evaluated if r.is_positive and r.classification in {"solo", "small"})
    corporate_count = sum(1 for r in evaluated if r.is_positive and r.classification == "corporate")
    unknown_count = sum(1 for r in evaluated if r.is_positive and r.classification == "unknown")
    city_missing_count = sum(1 for r in evaluated if r.is_positive and (not r.city_detected))

    rates = {
        "solo_rate": _safe_ratio(solo_count, positive_leads),
        "corporate_rate": _safe_ratio(corporate_count, positive_leads),
        "unknown_rate": _safe_ratio(unknown_count, positive_leads),
        "bad_domain_mix": _safe_ratio(noise_leads, total_leads),
        "city_missing_rate": _safe_ratio(city_missing_count, positive_leads),
    }

    phase1_complete, blocking = phase_status(rates, thresholds.phase1)
    current_phase = 1 if not phase1_complete else 2

    ranked = sorted(
        evaluated,
        key=lambda r: (-r.rank_key_score, r.domain, r.shop_name, r.url),
    )
    top50 = ranked[:50]
    top50_good_count = sum(1 for r in top50 if r.label == "○")
    top50_effective_good_count = sum(
        1 for r in top50 if r.label == "○" and (not _is_forced_noise_for_top50(r))
    )
    top50_bad_domain_count = sum(1 for r in top50 if r.is_noise)
    top50_city_missing_count = sum(1 for r in top50 if not r.city_detected)

    sample = [
        {
            "rank": idx + 1,
            "domain": row.domain,
            "shop_name": row.shop_name,
            "label": row.label,
            "reason": row.label_reason or row.noise_reason or "",
        }
        for idx, row in enumerate(top50[:10])
    ]

    ts = current_jst_timestamp()
    payload = {
        "schema_version": "1.0",
        "run": {
            "run_id": run_id_from_timestamp(ts),
            "timestamp_jst": ts,
            "input_csv": input_csv,
            "input_row_count": total_leads,
            "slice": slice_value,
            "git_commit": get_git_commit(ROOT),
            "config_hash": build_config_hash(governance, thresholds),
        },
        "phase": {
            "current_phase": current_phase,
            "phase_complete": phase1_complete,
            "blocking_kpis": blocking,
        },
        "counts": {
            "total_leads": total_leads,
            "noise_leads": noise_leads,
            "positive_leads": positive_leads,
            "positive_quality_leads": positive_quality_leads,
            "top50_count": 50,
        },
        "rates": rates,
        "top50": {
            "top50_good_count": top50_good_count,
            "top50_effective_good_count": top50_effective_good_count,
            "top50_bad_domain_count": top50_bad_domain_count,
            "top50_city_missing_count": top50_city_missing_count,
            "sample": sample,
        },
        "thresholds": {
            "phase1": {
                "solo_rate_min": thresholds.phase1.solo_rate_min,
                "corporate_rate_max": thresholds.phase1.corporate_rate_max,
                "unknown_rate_max": thresholds.phase1.unknown_rate_max,
                "bad_domain_mix_max": thresholds.phase1.bad_domain_mix_max,
                "city_missing_rate_max": thresholds.phase1.city_missing_rate_max,
            }
        },
        "diagnostics": {
            "bad_domains_top": [
                {"domain": d, "count": c} for d, c in bad_domains_counter.most_common(10)
            ],
            "noise_reasons_top": [
                {"reason": r, "count": c} for r, c in noise_reasons_counter.most_common(10)
            ],
            "missing_city_examples": missing_city_examples[:10],
            "unknown_examples_top": unknown_examples[:10],
            "notes": _dedupe_notes(notes),
        },
    }

    # Governance-provenance notes.
    if not governance.get("CEO.md"):
        payload["diagnostics"]["notes"].append("missing_governance_file:CEO.md")
    if not governance.get("OPS.md"):
        payload["diagnostics"]["notes"].append("missing_governance_file:OPS.md")
    if not governance.get("KPI.md"):
        payload["diagnostics"]["notes"].append("missing_governance_file:KPI.md")

    return payload


def recommend_patch_target(kpi: dict[str, Any]) -> tuple[str, str]:
    rates = kpi["rates"]
    top50 = kpi["top50"]
    thresholds = kpi["thresholds"]["phase1"]

    if rates["bad_domain_mix"] > thresholds["bad_domain_mix_max"]:
        return (
            "domain",
            "Expand noise-domain/path exclusion list and penalties in filtering/scoring (precision-first).",
        )
    if top50["top50_good_count"] < 30:
        return (
            "scoring",
            "Increase owner-operator signal weighting and media/portal penalties in ranking for Top50 quality.",
        )
    if rates["city_missing_rate"] > thresholds["city_missing_rate_max"]:
        return (
            "query",
            "Add stronger local-intent query constraints and city extraction fallback to reduce city-missing positives.",
        )
    return (
        "scoring",
        "Fine-tune precision weighting with minimal penalty adjustments; keep architecture unchanged.",
    )


def write_report(report_path: Path, kpi: dict[str, Any]) -> None:
    phase = kpi["phase"]
    rates = kpi["rates"]
    counts = kpi["counts"]
    top50 = kpi["top50"]
    p1 = kpi["thresholds"]["phase1"]

    bottleneck = phase["blocking_kpis"][0] if phase["blocking_kpis"] else "none"
    patch_target, patch_desc = recommend_patch_target(kpi)

    lines = [
        "# KPI Report",
        "",
        f"- Run ID: `{kpi['run']['run_id']}`",
        f"- Timestamp (JST): `{kpi['run']['timestamp_jst']}`",
        f"- Input: `{kpi['run']['input_csv']}`",
        f"- Slice: `{kpi['run']['slice']}`",
        "",
        "## Phase Status",
        "",
        f"- Current Phase: `{phase['current_phase']}`",
        f"- Phase 1 Complete: `{phase['phase_complete']}`",
        f"- Blocking KPIs: `{', '.join(phase['blocking_kpis']) if phase['blocking_kpis'] else 'none'}`",
        "",
        "## KPI Table (Phase 1)",
        "",
        "| KPI | Actual | Threshold | Status |",
        "|---|---:|---:|---|",
        f"| solo_rate | {rates['solo_rate']:.6f} | >= {p1['solo_rate_min']:.6f} | {'PASS' if rates['solo_rate'] >= p1['solo_rate_min'] else 'FAIL'} |",
        f"| corporate_rate | {rates['corporate_rate']:.6f} | <= {p1['corporate_rate_max']:.6f} | {'PASS' if rates['corporate_rate'] <= p1['corporate_rate_max'] else 'FAIL'} |",
        f"| unknown_rate | {rates['unknown_rate']:.6f} | <= {p1['unknown_rate_max']:.6f} | {'PASS' if rates['unknown_rate'] <= p1['unknown_rate_max'] else 'FAIL'} |",
        f"| bad_domain_mix | {rates['bad_domain_mix']:.6f} | <= {p1['bad_domain_mix_max']:.6f} | {'PASS' if rates['bad_domain_mix'] <= p1['bad_domain_mix_max'] else 'FAIL'} |",
        f"| city_missing_rate | {rates['city_missing_rate']:.6f} | <= {p1['city_missing_rate_max']:.6f} | {'PASS' if rates['city_missing_rate'] <= p1['city_missing_rate_max'] else 'FAIL'} |",
        "",
        "## Core Counts",
        "",
        f"- total_leads: `{counts['total_leads']}`",
        f"- noise_leads: `{counts['noise_leads']}`",
        f"- positive_leads: `{counts['positive_leads']}`",
        f"- positive_quality_leads: `{counts['positive_quality_leads']}`",
        f"- top50_good_count: `{top50['top50_good_count']}`",
        f"- top50_effective_good_count: `{top50['top50_effective_good_count']}`",
        f"- top50_bad_domain_count: `{top50['top50_bad_domain_count']}`",
        "",
        "## Bottleneck",
        "",
        f"- `{bottleneck}`",
        "",
        "## Recommended Minimal Precision-First Patch (Do Next, Not Here)",
        "",
        f"- Target: `{patch_target}`",
        f"- Action: {patch_desc}",
        "",
    ]

    if rates["unknown_rate"] > p1["unknown_rate_max"]:
        unknown_examples = kpi.get("diagnostics", {}).get("unknown_examples_top", [])
        lines.extend(
            [
                "## Unknown Examples (Top, For Debug)",
                "",
                "| domain | shop_name | marker_hit | marker |",
                "|---|---|---:|---|",
            ]
        )
        for item in unknown_examples:
            lines.append(
                f"| {item.get('domain','')} | {item.get('shop_name','')} | {item.get('marker_hit','')} | {item.get('marker','')} |"
            )
        lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")


def validate_payload_shape(kpi: dict[str, Any]) -> None:
    required_top = [
        "schema_version",
        "run",
        "phase",
        "counts",
        "rates",
        "top50",
        "thresholds",
        "diagnostics",
    ]
    for key in required_top:
        if key not in kpi:
            raise ValueError(f"missing top-level key: {key}")

    run_required = ["run_id", "timestamp_jst", "input_csv", "input_row_count", "slice", "git_commit", "config_hash"]
    for key in run_required:
        if key not in kpi["run"]:
            raise ValueError(f"missing run key: {key}")

    phase_required = ["current_phase", "phase_complete", "blocking_kpis"]
    for key in phase_required:
        if key not in kpi["phase"]:
            raise ValueError(f"missing phase key: {key}")

    count_required = ["total_leads", "noise_leads", "positive_leads", "positive_quality_leads", "top50_count"]
    for key in count_required:
        if key not in kpi["counts"]:
            raise ValueError(f"missing counts key: {key}")

    rates_required = ["solo_rate", "corporate_rate", "unknown_rate", "bad_domain_mix", "city_missing_rate"]
    for key in rates_required:
        if key not in kpi["rates"]:
            raise ValueError(f"missing rates key: {key}")

    top50_required = [
        "top50_good_count",
        "top50_effective_good_count",
        "top50_bad_domain_count",
        "top50_city_missing_count",
        "sample",
    ]
    for key in top50_required:
        if key not in kpi["top50"]:
            raise ValueError(f"missing top50 key: {key}")

    if "phase1" not in kpi["thresholds"]:
        raise ValueError("missing thresholds.phase1")

    diag_required = ["bad_domains_top", "noise_reasons_top", "missing_city_examples", "unknown_examples_top", "notes"]
    for key in diag_required:
        if key not in kpi["diagnostics"]:
            raise ValueError(f"missing diagnostics key: {key}")


def run(input_path: Path, out_path: Path, report_path: Path, slice_value: int | None) -> dict[str, Any]:
    governance = load_governance(ROOT)
    thresholds = parse_kpi_thresholds(governance.get("KPI.md", ""))
    rows = read_csv_rows(input_path)
    if slice_value is not None:
        rows = rows[:slice_value]

    kpi = compute_kpi_payload(
        rows=rows,
        input_csv=str(input_path),
        slice_value=slice_value,
        governance=governance,
        thresholds=thresholds,
    )
    validate_payload_shape(kpi)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(kpi, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(report_path, kpi)
    return kpi


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    out_path = Path(args.out)
    report_path = Path(args.report)

    if args.slice is not None and args.slice <= 0:
        print("[ERROR] --slice must be > 0", file=sys.stderr)
        return 2

    if not input_path.exists():
        print(f"[ERROR] Input CSV not found: {input_path}", file=sys.stderr)
        return 1

    try:
        kpi = run(input_path=input_path, out_path=out_path, report_path=report_path, slice_value=args.slice)
    except Exception as exc:
        print(f"[ERROR] KPI generation failed: {exc}", file=sys.stderr)
        return 1

    print(
        "[OK] KPI generated "
        f"(phase_complete={kpi['phase']['phase_complete']}, "
        f"blocking={kpi['phase']['blocking_kpis']}, "
        f"out={out_path}, report={report_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
