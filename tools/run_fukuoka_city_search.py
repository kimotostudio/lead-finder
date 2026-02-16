#!/usr/bin/env python3
"""
Fukuoka-city focused discovery runner.

- Expands search queries from config/search_terms_fukuoka.json
- Collects candidate URLs deterministically
- Processes leads through existing pipeline (crawl -> score -> filter)
- Outputs outreach-oriented CSV to web_app/output/leads_fukuoka_city_<timestamp>.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import random
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - optional dependency
    BeautifulSoup = None

from src.utils.url_filter import normalize_url

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "search_terms_fukuoka.json"
DEFAULT_OUTPUT_DIR = ROOT / "web_app" / "output"
DEFAULT_TIMEOUT = 15
DEFAULT_MAX_RETRIES = 3
DEFAULT_RATE_LIMIT_DELAY = 2.0
DEFAULT_PARALLEL_WORKERS = 6

CONTACT_PATTERNS = (
    "お問い合わせ",
    "お問合せ",
    "問い合わせ",
    "contact",
    "inquiry",
    "ご予約",
    "予約",
    "reservation",
)

FORM_PATTERNS = (
    "form",
    "フォーム",
    "予約フォーム",
    "contact-form",
    "reserva",
    "airreserve",
)

LINE_PATTERNS = ("line.me", "lin.ee", "line://", "友だち追加")

OUTPUT_COLUMNS = [
    "domain",
    "url",
    "title",
    "category_guess",
    "has_contact_page",
    "contact_url",
    "has_form",
    "form_url",
    "has_line",
    "address",
    "area_guess",
    "solo_score",
    "reason",
]


def load_search_config(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if "city" not in data:
        raise ValueError(f"Missing keys in {path}: ['city']")
    # Backward/forward-compatible:
    # - preferred: explicit `queries`
    # - fallback: layered anchors
    has_explicit_queries = bool(data.get("queries"))
    has_legacy_layers = all(k in data for k in ("wards", "areas", "business_types", "hidden_terms", "action_terms"))
    has_new_layers = all(k in data for k in ("region_anchors", "business_anchors", "solo_signals", "booking_signals"))
    if not (has_explicit_queries or has_legacy_layers or has_new_layers):
        raise ValueError(
            f"Missing query inputs in {path}: expected one of ['queries'] or layered anchors "
            "(['region_anchors','business_anchors','solo_signals','booking_signals'])"
        )
    return data


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        key = " ".join(str(v).split())
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _query_contains_negative(query: str, negatives: list[str]) -> bool:
    q = query.lower()
    return any(n.lower() in q for n in negatives if n)


def _passes_required_markers(query: str, required_markers: Any) -> bool:
    if not required_markers:
        return True
    q = query.lower()
    if isinstance(required_markers, list):
        return any(str(m).lower() in q for m in required_markers if m)
    if isinstance(required_markers, dict):
        # Each group must match at least one marker.
        for _, markers in required_markers.items():
            if not markers:
                continue
            if not any(str(m).lower() in q for m in markers):
                return False
        return True
    return True


def apply_query_filters(queries: list[str], negatives: list[str], required_markers: Any) -> list[str]:
    out: list[str] = []
    for q in queries:
        if _query_contains_negative(q, negatives):
            continue
        if not _passes_required_markers(q, required_markers):
            continue
        out.append(q)
    return out


def build_fukuoka_queries(cfg: dict[str, Any], max_queries: int) -> list[str]:
    city = str(cfg["city"]).strip()
    negatives = [str(x).strip() for x in cfg.get("negatives", [])]
    required_markers = cfg.get("required_markers", {})

    queries: list[str] = []
    explicit_queries = [str(x).strip() for x in cfg.get("queries", []) if str(x).strip()]
    if explicit_queries:
        queries.extend(explicit_queries)
    else:
        # Backward compatibility: old key names
        if "region_anchors" in cfg:
            region_anchors = cfg.get("region_anchors", {})
            wards = [str(x).strip() for x in region_anchors.get("wards", [])]
            areas = [str(x).strip() for x in region_anchors.get("areas", [])]
        else:
            wards = [str(x).strip() for x in cfg.get("wards", [])]
            areas = [str(x).strip() for x in cfg.get("areas", [])]

        business_types = [str(x).strip() for x in cfg.get("business_anchors", cfg.get("business_types", []))]
        hidden_terms = [str(x).strip() for x in cfg.get("solo_signals", cfg.get("hidden_terms", []))]
        action_terms = [str(x).strip() for x in cfg.get("booking_signals", cfg.get("action_terms", []))]
        site_modifiers = [str(x).strip() for x in cfg.get("site_modifiers", [])]
        templates = [str(x).strip() for x in cfg.get("query_templates", [])]
        if not templates:
            templates = [
                "{city} {ward} {biz} {solo} {booking}",
                "{city} {area} {biz} {solo} {booking}",
                "{city} {ward} {biz} {solo}",
                "{city} {area} {biz} {booking}",
                "{city} {biz} {solo} {booking}",
            ]

        for tpl in templates:
            if "{ward}" in tpl:
                for ward in wards:
                    for biz in business_types:
                        for solo in hidden_terms:
                            for booking in action_terms:
                                queries.append(
                                    tpl.format(city=city, ward=ward, area=ward, biz=biz, solo=solo, booking=booking).strip()
                                )
            elif "{area}" in tpl:
                for area in areas:
                    for biz in business_types:
                        for solo in hidden_terms:
                            for booking in action_terms:
                                queries.append(
                                    tpl.format(city=city, ward=area, area=area, biz=biz, solo=solo, booking=booking).strip()
                                )
            else:
                for biz in business_types:
                    for solo in hidden_terms:
                        for booking in action_terms:
                            queries.append(
                                tpl.format(city=city, biz=biz, solo=solo, booking=booking).strip()
                            )

        for biz in business_types:
            for solo in hidden_terms:
                for action in action_terms:
                    for site in site_modifiers:
                        queries.append(f"{city} {biz} {solo} {action} {site}")

    queries = _dedupe_preserve_order(queries)
    queries = apply_query_filters(queries, negatives=negatives, required_markers=required_markers)
    if max_queries > 0:
        queries = queries[:max_queries]
    return queries


def sort_urls_deterministically(urls: list[str]) -> list[str]:
    canonical: dict[str, str] = {}
    for raw in urls:
        n = normalize_url(raw)
        current = canonical.get(n)
        if current is None or raw < current:
            canonical[n] = raw

    def _key(url: str) -> tuple[str, str]:
        n = normalize_url(url)
        p = urlparse(n)
        return (p.netloc.lower().replace("www.", ""), n)

    return sorted(canonical.values(), key=_key)


def _find_first_link(soup: Any, base_url: str, patterns: tuple[str, ...]) -> str:
    if soup is None:
        return ""
    pats = tuple(p.lower() for p in patterns)
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        text = (a.get_text(" ", strip=True) or "").strip()
        blob = f"{href} {text}".lower()
        if any(p in blob for p in pats):
            if href.startswith(("http://", "https://")):
                return href
            return urljoin(base_url, href)
    return ""


def _extract_output_row(lead: dict[str, Any], areas: list[str]) -> dict[str, Any]:
    url = str(lead.get("url", ""))
    title = str(lead.get("title", "") or "")
    html = str(lead.get("html", "") or "")
    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser") if html else BeautifulSoup("", "html.parser")
    else:
        soup = None

    contact_url = _find_first_link(soup, url, CONTACT_PATTERNS)
    form_url = _find_first_link(soup, url, FORM_PATTERNS)
    has_form = bool(form_url) or ("<form" in html.lower())

    blob = f"{url} {title} {lead.get('visible_text', '')} {lead.get('address', '')}"
    blob_l = blob.lower()
    has_line = any(p in blob_l for p in LINE_PATTERNS)

    area_guess = ""
    for a in areas:
        if a and a in blob:
            area_guess = a
            break

    category_guess = str(lead.get("business_type") or lead.get("site_type") or "unknown")
    solo_score = lead.get("solo_score_100", lead.get("solo_score", ""))

    reason_tokens = []
    for key in ("solo_classification", "reasons", "boost_reasons"):
        value = str(lead.get(key, "") or "").strip()
        if value:
            reason_tokens.append(value)

    return {
        "domain": str(lead.get("domain", "")),
        "url": url,
        "title": title,
        "category_guess": category_guess,
        "has_contact_page": bool(contact_url),
        "contact_url": contact_url,
        "has_form": bool(has_form),
        "form_url": form_url,
        "has_line": bool(has_line),
        "address": str(lead.get("address", "")),
        "area_guess": area_guess or str(lead.get("city", "")),
        "solo_score": solo_score,
        "reason": " | ".join(reason_tokens),
    }


def run_fukuoka_search(
    *,
    config_path: Path,
    output_dir: Path,
    max_queries: int,
    max_results_per_query: int,
    parallel_workers: int,
    sleep_sec: float,
    seed: int,
    max_empty_queries: int,
    bootstrap_csv: Path | None,
    max_process_urls: int,
) -> tuple[Path, dict[str, Any]]:
    try:
        from config.settings import MAX_RETRIES, RATE_LIMIT_DELAY, TIMEOUT
    except Exception:
        MAX_RETRIES = DEFAULT_MAX_RETRIES
        RATE_LIMIT_DELAY = DEFAULT_RATE_LIMIT_DELAY
        TIMEOUT = DEFAULT_TIMEOUT
    from src.engines.multi_engine import MultiEngineSearch
    from src.processor import LeadProcessor

    cfg = load_search_config(config_path)
    random.seed(seed)

    logger = logging.getLogger("fukuoka_search")
    logger.info("search config=%s", config_path)
    logger.info(
        "runtime fixed params: seed=%s timeout=%s retries=%s rate_limit_delay=%s parallel_workers=%s",
        seed,
        TIMEOUT,
        MAX_RETRIES,
        RATE_LIMIT_DELAY,
        parallel_workers,
    )

    queries = build_fukuoka_queries(cfg, max_queries=max_queries)
    logger.info("query_count=%d", len(queries))

    searcher = MultiEngineSearch()
    all_urls: list[str] = []
    top_domains_counter: Counter[str] = Counter()
    empty_streak = 0
    query_success_count = 0
    query_empty_count = 0
    query_error_count = 0

    for i, q in enumerate(queries, start=1):
        logger.info("[%d/%d] query=%s", i, len(queries), q)
        try:
            urls = searcher.search(q, max_results_per_engine=max_results_per_query)
        except Exception as exc:
            logger.warning("search failed: %s", exc)
            urls = []
            query_error_count += 1
        if urls:
            query_success_count += 1
        else:
            query_empty_count += 1
        if not urls:
            empty_streak += 1
        else:
            empty_streak = 0
        all_urls.extend(urls)
        for u in urls:
            domain = urlparse(u).netloc.lower().replace("www.", "")
            if domain:
                top_domains_counter[domain] += 1
        if max_empty_queries > 0 and empty_streak >= max_empty_queries:
            logger.warning("early stop: consecutive empty queries reached %d", max_empty_queries)
            break
        time.sleep(max(0.0, sleep_sec))

    if not all_urls and bootstrap_csv and bootstrap_csv.exists():
        logger.warning("no URLs from live search; bootstrapping from %s", bootstrap_csv)
        with bootstrap_csv.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                u = (
                    row.get("URL")
                    or row.get("url")
                    or row.get("final_url")
                    or row.get("最終URL")
                    or ""
                ).strip()
                if u:
                    all_urls.append(u)
        logger.info("bootstrap URLs loaded=%d", len(all_urls))

    sorted_urls = sort_urls_deterministically(all_urls)
    if max_process_urls > 0:
        sorted_urls = sorted_urls[:max_process_urls]
    logger.info("urls collected raw=%d unique_sorted=%d", len(all_urls), len(sorted_urls))

    processor = LeadProcessor(parallel_workers=parallel_workers, disable_progress=True)
    raw_leads, failed_urls = processor.process_urls(sorted_urls)
    deduped = processor.deduplicate_leads(raw_leads)
    kept, filtered = processor.filter_and_boost(deduped)

    # Final deterministic ordering by domain then score desc.
    kept_sorted = sorted(
        kept,
        key=lambda x: (str(x.get("domain", "")), -int(x.get("score") or 0), str(x.get("url", ""))),
    )

    rows = [_extract_output_row(lead, [str(a) for a in cfg.get("areas", [])]) for lead in kept_sorted]

    # Safety dedupe by domain for final sales output.
    seen_domains: set[str] = set()
    dedup_rows: list[dict[str, Any]] = []
    for row in rows:
        domain = row["domain"].strip().lower()
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)
        dedup_rows.append(row)

    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = output_dir / f"leads_fukuoka_city_{ts}.csv"

    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(dedup_rows)

    run_meta = {
        "output_csv": str(out_csv),
        "query_count": len(queries),
        "raw_url_count": len(all_urls),
        "unique_sorted_url_count": len(sorted_urls),
        "raw_leads": len(raw_leads),
        "deduped_leads": len(deduped),
        "kept_leads": len(kept),
        "filtered_leads": len(filtered),
        "failed_urls": len(failed_urls),
        "final_rows": len(dedup_rows),
        "seed": seed,
        "query_success_count": query_success_count,
        "query_empty_count": query_empty_count,
        "query_error_count": query_error_count,
        "search_vocab_hash": hashlib.sha256(json.dumps(queries, ensure_ascii=False).encode("utf-8")).hexdigest()[:16],
        "top_domains_sample": [d for d, _ in top_domains_counter.most_common(10)],
        "max_empty_queries": max_empty_queries,
        "bootstrap_csv": str(bootstrap_csv) if bootstrap_csv else "",
        "max_process_urls": max_process_urls,
        "parallel_workers": parallel_workers,
        "timeout": TIMEOUT,
        "max_retries": MAX_RETRIES,
    }

    meta_path = out_csv.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(run_meta, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("output_csv=%s rows=%d", out_csv, len(dedup_rows))
    logger.info("meta=%s", meta_path)
    return out_csv, run_meta


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Fukuoka city search and export outreach CSV")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to Fukuoka search term config")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory")
    parser.add_argument("--max-queries", type=int, default=0, help="Max number of generated queries (0: use config default)")
    parser.add_argument("--max-results-per-query", type=int, default=0, help="Max search URLs per query (0: use config default)")
    parser.add_argument("--parallel-workers", type=int, default=0, help="Parallel workers for processing (0: use config default)")
    parser.add_argument("--sleep-sec", type=float, default=-1.0, help="Sleep seconds between queries (-1: use config default)")
    parser.add_argument("--seed", type=int, default=0, help="Deterministic seed (0: use config default)")
    parser.add_argument("--max-empty-queries", type=int, default=5, help="Early-stop when consecutive queries return 0 URLs (0 to disable)")
    parser.add_argument("--bootstrap-csv", default="", help="Fallback CSV path to load URLs when live search returns no URLs")
    parser.add_argument("--max-process-urls", type=int, default=40, help="Cap number of URLs to process after deterministic sort (0 to disable)")
    parser.add_argument("--verbose", action="store_true", help="Verbose logs")
    return parser.parse_args()


def main() -> int:
    try:
        from config.settings import PARALLEL_WORKERS
    except Exception:
        PARALLEL_WORKERS = DEFAULT_PARALLEL_WORKERS
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = load_search_config(Path(args.config))
    max_queries = args.max_queries or int(cfg.get("max_queries_default", 300))
    max_results = args.max_results_per_query or int(cfg.get("max_results_per_query_default", 5))
    parallel_workers = args.parallel_workers or int(cfg.get("parallel_workers_default", PARALLEL_WORKERS))
    sleep_sec = args.sleep_sec if args.sleep_sec >= 0.0 else float(cfg.get("sleep_sec_between_queries_default", 1.0))
    seed = args.seed or int(cfg.get("seed", 20260216))

    out_csv, meta = run_fukuoka_search(
        config_path=Path(args.config),
        output_dir=Path(args.output_dir),
        max_queries=max_queries,
        max_results_per_query=max_results,
        parallel_workers=parallel_workers,
        sleep_sec=sleep_sec,
        seed=seed,
        max_empty_queries=args.max_empty_queries,
        bootstrap_csv=Path(args.bootstrap_csv) if args.bootstrap_csv else None,
        max_process_urls=args.max_process_urls,
    )

    print(out_csv)
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
