#!/usr/bin/env python3
"""
Run searches for each Tokyo ward (区) sequentially with a cap on queries per ward.

This is a lightweight wrapper that builds queries for each city in
`TARGET_REGIONS['東京']`, limits to `--queries-per-ward`, then runs the
search + processing steps (similar to `advanced_search.py`) and writes a
CSV per ward under `output/`.

Intended for medium-scale, staggered runs to avoid rate limits.
"""
import argparse
import logging
import time
import os
import sys

from config.advanced_queries import (
    TARGET_REGIONS,
    LAYER_A_PATTERNS,
    LAYER_B_PLATFORMS,
    LAYER_C_PATTERNS,
    LAYER_D_WELLNESS,
)
from src.engines.multi_engine import MultiEngineSearch
from src.processor import LeadProcessor
from src.output_writer import OutputWriter

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Romanized (ascii) -> Japanese ward mapping for Tokyo
ROMAN_TO_JP = {
    'shinjuku': '新宿区',
    'shibuya': '渋谷区',
    'setagaya': '世田谷区',
    'nerima': '練馬区',
    'ota': '大田区',
    'oota': '大田区',
    'adachi': '足立区',
    'edogawa': '江戸川区',
    'suginami': '杉並区',
    'itabashi': '板橋区',
    'koto': '江東区',
    'shinagawa': '品川区',
    'meguro': '目黒区',
    'nakano': '中野区',
    'kita': '北区',
    'toshima': '豊島区',
}


def build_queries_for_city(city: str) -> list:
    q = []
    # Layer A
    for pattern in LAYER_A_PATTERNS:
        q.append(pattern.format(city=city))
    # Layer B
    for platform_patterns in LAYER_B_PLATFORMS.values():
        for pattern in platform_patterns:
            q.append(pattern.format(city=city))
    # Layer C
    for pattern in LAYER_C_PATTERNS:
        q.append(pattern.format(city=city))
    # Layer D
    for pattern in LAYER_D_WELLNESS:
        q.append(pattern.format(city=city))
    return q


def safe_filename(s: str) -> str:
    return s.replace(' ', '_').replace('/', '_')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--queries-per-ward', type=int, default=100, help='Max queries per ward')
    p.add_argument('--limit', type=int, default=5, help='URLs per query (passed to searcher)')
    p.add_argument('--parallel', type=int, default=3, help='Parallel workers for processing')
    p.add_argument('--sleep', type=float, default=1.0, help='Seconds to sleep between queries')
    p.add_argument('--outdir', type=str, default='output', help='Output directory')
    p.add_argument('--dry-run', action='store_true', help='If set, do not push to sheets (default behavior)')
    p.add_argument('--only', nargs='+', help='Optional list of city names to run (e.g. 杉並区 大田区)')
    args = p.parse_args()

    if '東京' not in TARGET_REGIONS:
        logger.error('Tokyo not configured in TARGET_REGIONS')
        sys.exit(1)

    os.makedirs(args.outdir, exist_ok=True)

    cities = TARGET_REGIONS['東京']
    # If --only specified, filter cities (accept either full Japanese name or romanized ascii key)
    if args.only:
        requested = set()
        for key in args.only:
            low = key.lower()
            if low in ROMAN_TO_JP:
                requested.add(ROMAN_TO_JP[low])
            else:
                # accept Japanese full name as provided
                requested.add(key)

        filtered = [c for c in cities if c in requested]
        if not filtered:
            logger.error('No matching cities found for --only: %s', args.only)
            logger.info('Available Tokyo cities: %s', ', '.join(cities))
            sys.exit(1)
        cities = filtered
    searcher = MultiEngineSearch()

    for city in cities:
        logger.info('=== City: %s ===', city)
        queries = build_queries_for_city(city)[: args.queries_per_ward]
        logger.info('Queries for %s: %d (capped)', city, len(queries))

        all_urls = set()
        for i, q in enumerate(queries, 1):
            logger.info('[%d/%d] %s', i, len(queries), q if isinstance(q, str) else str(q))
            logger.info('  Query: %s', q)
            try:
                urls = searcher.search(q, max_results_per_engine=args.limit)
                logger.info('  Found %d URLs', len(urls))
                all_urls.update(urls)
            except Exception as e:
                logger.exception('  Search error: %s', e)
            time.sleep(args.sleep)

        logger.info('Total unique URLs collected for %s: %d', city, len(all_urls))
        if not all_urls:
            logger.warning('No URLs for %s - skipping processing', city)
            continue

        # Process URLs
        processor = LeadProcessor(parallel_workers=args.parallel)
        leads, failed_urls = processor.process_urls(list(all_urls))
        logger.info('Leads before dedupe: %d', len(leads))
        unique = processor.deduplicate_leads(leads)
        logger.info('Leads after dedupe: %d', len(unique))

        # Sort and write
        unique.sort(key=lambda x: x.get('score', 0), reverse=True)
        outpath = os.path.join(args.outdir, f"advanced_{safe_filename(city)}_ward.csv")
        logger.info('Writing results to: %s', outpath)
        OutputWriter.write_csv(unique, outpath)

        # failed urls
        failed_path = outpath.replace('.csv', '_failed_urls.txt')
        OutputWriter.write_failed_urls(failed_urls, failed_path)

    logger.info('All wards processed.')


if __name__ == '__main__':
    main()
