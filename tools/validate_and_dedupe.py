#!/usr/bin/env python3
"""
URL Validation and Deduplication Script (Step 3)

Post-processes scraped CSV files:
1. Validates URL liveness (alive/dead check)
2. Resolves redirects and stores final_url
3. Deduplicates by normalized domain (based on final_url)
4. Outputs clean, sales-ready CSVs per region

Input: scraping_<region>.csv files
Output: scraping_<region>_clean.csv files (only alive, unique by domain)

Usage:
    python tools/validate_and_dedupe.py --input ./output --output ./cleaned
    python tools/validate_and_dedupe.py --input ./web_app/output --output ./web_app/output/cleaned
    python tools/validate_and_dedupe.py --keep-dead  # Keep dead URLs in output
"""
import argparse
import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.liveness import (
    LivenessChecker,
    check_leads_liveness,
    dedupe_by_domain,
    extract_domain,
)
from src.normalize import normalize_url_strict, FINAL_SCHEMA, HEADER_LABELS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# Dead URL log file
DEAD_URL_LOG = 'logs/dead_urls.log'


def setup_dead_url_logger() -> logging.Logger:
    """Setup a separate logger for dead URLs."""
    dead_logger = logging.getLogger('dead_urls')
    dead_logger.setLevel(logging.INFO)

    # Create logs directory
    os.makedirs('logs', exist_ok=True)

    # File handler for dead URLs
    fh = logging.FileHandler(DEAD_URL_LOG, encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    dead_logger.addHandler(fh)

    return dead_logger


def read_csv(filepath: str) -> List[Dict]:
    """
    Read CSV file and return list of dicts.

    Handles both UTF-8 and UTF-8-BOM encodings.
    """
    leads = []

    # Try UTF-8-BOM first, then UTF-8
    for encoding in ['utf-8-sig', 'utf-8', 'cp932', 'shift_jis']:
        try:
            with open(filepath, 'r', encoding=encoding, newline='') as f:
                reader = csv.DictReader(f)
                leads = list(reader)
                logger.info(f"Read {len(leads)} rows from {filepath} (encoding: {encoding})")
                return leads
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.error(f"Error reading {filepath}: {e}")
            raise

    raise ValueError(f"Could not read {filepath} with any supported encoding")


def write_csv(leads: List[Dict], filepath: str, fieldnames: Optional[List[str]] = None):
    """
    Write leads to CSV file with UTF-8-BOM encoding.
    """
    if not leads:
        logger.warning(f"No leads to write to {filepath}")
        return

    # Determine fieldnames
    if fieldnames is None:
        # Use keys from first lead, maintaining order
        fieldnames = list(leads[0].keys())

    # Ensure output directory exists
    os.makedirs(os.path.dirname(filepath) or '.', exist_ok=True)

    with open(filepath, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)

        # Write header (use Japanese labels if available)
        header_row = {col: HEADER_LABELS.get(col, col) for col in fieldnames}
        writer.writerow(header_row)

        # Write data rows
        for lead in leads:
            # Ensure all fields exist
            row = {col: lead.get(col, '') for col in fieldnames}
            writer.writerow(row)

    logger.info(f"Wrote {len(leads)} leads to {filepath}")


def map_csv_to_schema(lead: Dict) -> Dict:
    """
    Map CSV row to normalized schema, handling various header formats.
    """
    # Common header mappings (Japanese -> English)
    header_map = {
        '店舗名': 'store_name',
        'URL': 'url',
        '元のURL': 'url',
        'コメント': 'comment',
        'スコア': 'score',
        '点数': 'score',
        '地方': 'region',
        '市区町村': 'city',
        '業種': 'business_type',
        'サイト種別': 'site_type',
        '電話番号': 'phone',
        'メール': 'email',
        '検索クエリ': 'source_query',
        '取得日時': 'fetched_at_iso',
        '判定': 'grade',
        '判定理由': 'reasons',
        'HTTPステータス': 'http_status',
        '最終URL': 'final_url',
        '生存': 'is_alive',
        'チェック日時': 'checked_at_iso',
    }

    mapped = {}
    for key, value in lead.items():
        # Try to map Japanese header to English
        mapped_key = header_map.get(key, key)
        mapped[mapped_key] = value

    # Ensure URL exists
    if 'url' not in mapped or not mapped['url']:
        # Try alternative fields
        for alt in ['Website URL', 'website', 'URL', 'url']:
            if alt in lead and lead[alt]:
                mapped['url'] = lead[alt]
                break

    # Ensure score is numeric
    if 'score' in mapped:
        try:
            mapped['score'] = int(mapped['score'])
        except (ValueError, TypeError):
            mapped['score'] = 0

    return mapped


def process_region_file(
    input_path: str,
    output_path: str,
    keep_dead: bool = False,
    max_workers: int = 10,
    dead_logger: Optional[logging.Logger] = None,
) -> Dict:
    """
    Process a single region CSV file.

    Returns:
        Dict with stats: total, alive, dead, unique
    """
    stats = {'total': 0, 'alive': 0, 'dead': 0, 'unique': 0, 'file': input_path}

    # Read input
    try:
        raw_leads = read_csv(input_path)
    except Exception as e:
        logger.error(f"Failed to read {input_path}: {e}")
        return stats

    stats['total'] = len(raw_leads)

    if not raw_leads:
        logger.warning(f"No leads in {input_path}")
        return stats

    # Map to schema
    leads = [map_csv_to_schema(lead) for lead in raw_leads]

    # Normalize URLs
    for lead in leads:
        if lead.get('url'):
            lead['url'] = normalize_url_strict(lead['url'])

    # Check liveness
    logger.info(f"Checking liveness for {len(leads)} leads...")

    def progress_callback(current, total, url, result):
        if current % 10 == 0 or current == total:
            logger.info(f"  Progress: {current}/{total}")
        if not result['is_alive'] and dead_logger:
            dead_logger.info(
                f"DEAD | {url} | final={result.get('final_url', '')} | "
                f"status={result.get('http_status', '')} | error={result.get('error', '')}"
            )

    checked_leads = check_leads_liveness(
        leads,
        keep_dead=True,  # Always check all, filter later
        max_workers=max_workers,
        progress_callback=progress_callback,
    )

    # Count alive/dead
    alive_leads = [l for l in checked_leads if l.get('is_alive')]
    dead_leads = [l for l in checked_leads if not l.get('is_alive')]
    stats['alive'] = len(alive_leads)
    stats['dead'] = len(dead_leads)

    # Deduplicate by domain (using final_url when available)
    if keep_dead:
        # Keep dead but still dedupe
        leads_to_dedupe = checked_leads
    else:
        # Only dedupe alive leads
        leads_to_dedupe = alive_leads

    deduped_leads = dedupe_by_domain(leads_to_dedupe, use_final_url=True)

    # Sort by score descending
    deduped_leads.sort(key=lambda x: int(x.get('score', 0)), reverse=True)

    stats['unique'] = len(deduped_leads)

    # Update status column
    for lead in deduped_leads:
        lead['status'] = 'OK' if lead.get('is_alive') else 'DEAD'

    # Determine output columns
    # Keep original columns + add liveness fields at the end
    output_columns = []
    if raw_leads:
        # Get original column order from first row
        for col in raw_leads[0].keys():
            mapped_col = map_csv_to_schema({col: ''}).keys()
            for mc in mapped_col:
                if mc not in output_columns:
                    output_columns.append(mc)

    # Ensure essential columns exist
    essential = ['store_name', 'url', 'score', 'http_status', 'final_url', 'is_alive', 'checked_at_iso']
    for col in essential:
        if col not in output_columns:
            output_columns.append(col)

    # Write output
    write_csv(deduped_leads, output_path, fieldnames=output_columns)

    return stats


def find_region_files(input_dir: str) -> List[str]:
    """Find all scraping_*.csv files in input directory."""
    input_path = Path(input_dir)
    patterns = ['scraping_*.csv', 'leads_*.csv', '*.csv']

    files = []
    for pattern in patterns:
        found = list(input_path.glob(pattern))
        # Exclude already-cleaned files
        found = [f for f in found if '_clean' not in f.name]
        files.extend(found)

    # Remove duplicates
    files = list(set(files))

    return [str(f) for f in files]


def main():
    parser = argparse.ArgumentParser(
        description='Validate URL liveness and deduplicate leads by domain'
    )
    parser.add_argument(
        '--input', '-i',
        default='./output',
        help='Input directory containing CSV files (default: ./output)'
    )
    parser.add_argument(
        '--output', '-o',
        default='./output/cleaned',
        help='Output directory for clean CSV files (default: ./output/cleaned)'
    )
    parser.add_argument(
        '--keep-dead',
        action='store_true',
        help='Keep dead URLs in output (marked with status=DEAD)'
    )
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=10,
        help='Number of concurrent workers for liveness checks (default: 10)'
    )
    parser.add_argument(
        '--file', '-f',
        help='Process a single file instead of directory'
    )

    args = parser.parse_args()

    # Setup dead URL logger
    dead_logger = setup_dead_url_logger()

    print("=" * 60)
    print("URL Validation & Deduplication Tool")
    print("=" * 60)

    if args.file:
        # Process single file
        files = [args.file]
    else:
        # Find all region files
        files = find_region_files(args.input)

    if not files:
        print(f"No CSV files found in {args.input}")
        return

    print(f"Found {len(files)} file(s) to process")
    print(f"Keep dead URLs: {args.keep_dead}")
    print(f"Workers: {args.workers}")
    print("-" * 60)

    # Process each file
    total_stats = {'total': 0, 'alive': 0, 'dead': 0, 'unique': 0}

    for filepath in files:
        filename = os.path.basename(filepath)
        # Generate output filename
        name_part = filename.replace('.csv', '')
        output_filename = f"{name_part}_clean.csv"
        output_path = os.path.join(args.output, output_filename)

        print(f"\nProcessing: {filename}")

        stats = process_region_file(
            filepath,
            output_path,
            keep_dead=args.keep_dead,
            max_workers=args.workers,
            dead_logger=dead_logger,
        )

        print(f"  Total: {stats['total']}")
        print(f"  Alive: {stats['alive']}")
        print(f"  Dead:  {stats['dead']}")
        print(f"  Unique (final): {stats['unique']}")
        print(f"  Output: {output_path}")

        # Accumulate stats
        for key in ['total', 'alive', 'dead', 'unique']:
            total_stats[key] += stats[key]

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total leads processed: {total_stats['total']}")
    print(f"Alive: {total_stats['alive']}")
    print(f"Dead:  {total_stats['dead']}")
    print(f"Unique (final): {total_stats['unique']}")
    print(f"Dead URL log: {DEAD_URL_LOG}")
    print("=" * 60)


if __name__ == '__main__':
    main()


# =============================================================================
# HOW TO RUN
# =============================================================================
#
# Basic usage (process all CSVs in ./output):
#   python tools/validate_and_dedupe.py
#
# Specify input/output directories:
#   python tools/validate_and_dedupe.py --input ./web_app/output --output ./web_app/output/cleaned
#
# Process a single file:
#   python tools/validate_and_dedupe.py --file ./output/leads_tokyo.csv
#
# Keep dead URLs in output (marked with status=DEAD):
#   python tools/validate_and_dedupe.py --keep-dead
#
# Adjust concurrency:
#   python tools/validate_and_dedupe.py --workers 20
#
# =============================================================================
