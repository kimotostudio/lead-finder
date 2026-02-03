#!/usr/bin/env python3
"""
Full-scale pipeline script - runs complete search for all cities in a region.

Usage:
  # Full search for Tokyo (all 15 wards)
  python tools/run_full_search.py tokyo

  # Full search with sheets push
  python tools/run_full_search.py tokyo --with-sheets --commit

  # Multiple regions
  python tools/run_full_search.py tokyo kanagawa --with-sheets --commit

Environment variables (for sheets):
  GOOGLE_APPLICATION_CREDENTIALS - path to service account JSON
  SHEETS_SPREADSHEET_ID - Google Spreadsheet ID
"""
import argparse
import os
import sys
import csv
from datetime import datetime

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.advanced_queries import generate_queries, TARGET_REGIONS
from src.engines.multi_engine import MultiEngineSearch
from src.processor import LeadProcessor
from src.normalize import normalize_leads, FINAL_SCHEMA
from src.liveness import check_leads_liveness, dedupe_by_domain


def print_section(title):
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def run_full_search(region_jp, urls_per_query=10, max_queries=None):
    """Run full search for the given region (all cities, all query layers)."""
    print_section(f"Step 1: Full Search for {region_jp}")

    # Generate ALL queries (no limit)
    queries = generate_queries(region_jp)

    # Limit queries if specified
    if max_queries and len(queries) > max_queries:
        queries = queries[:max_queries]
        print(f"Limited to {max_queries} queries (from {len(generate_queries(region_jp))} total)")

    print(f"Generated {len(queries)} queries for {region_jp}")
    print(f"Cities: {', '.join(TARGET_REGIONS[region_jp])}")

    # Run search
    searcher = MultiEngineSearch()
    all_urls = []

    for i, query in enumerate(queries):
        print(f"  [{i+1}/{len(queries)}] Searching: {query[:50]}...")
        try:
            urls = searcher.search(query, max_results_per_engine=urls_per_query)
            all_urls.extend(urls)
            if urls:
                print(f"    Found {len(urls)} URLs")
        except Exception as e:
            print(f"    Error: {e}")

    # Dedupe URLs
    unique_urls = list(set(all_urls))
    print(f"\nTotal URLs: {len(all_urls)} -> {len(unique_urls)} unique")

    return unique_urls


def process_urls(urls, region_jp, batch_size=50):
    """Process URLs through the lead processor."""
    print_section(f"Step 2: Process {len(urls)} URLs")

    processor = LeadProcessor()
    leads = []

    for i, url in enumerate(urls):
        if (i + 1) % batch_size == 0:
            print(f"  Progress: {i+1}/{len(urls)} ({len(leads)} valid leads so far)")
        try:
            result = processor.process_url(url)
            if result and result.get('score', 0) > 0:
                result['region'] = region_jp
                leads.append(result)
        except Exception as e:
            pass  # Silent fail for individual URLs

    print(f"\nProcessed {len(leads)} valid leads from {len(urls)} URLs")
    return leads


def normalize_and_dedupe(leads, region_jp, source_query="full"):
    """Normalize and dedupe leads."""
    print_section("Step 3: Normalize & Dedupe")

    normalized = normalize_leads(leads, source_query=source_query, region=region_jp)
    print(f"Normalized: {len(leads)} -> {len(normalized)} leads")

    return normalized


def check_liveness(leads, max_workers=10):
    """Check URL liveness."""
    print_section(f"Step 4: Liveness Check ({len(leads)} URLs)")

    checked = check_leads_liveness(
        leads,
        keep_dead=True,
        max_workers=max_workers,
        progress_callback=lambda c, t, u, r: print(f"  [{c}/{t}] {'ALIVE' if r['is_alive'] else 'DEAD'}") if c % 20 == 0 else None
    )

    alive = [l for l in checked if l.get('is_alive')]
    dead = [l for l in checked if not l.get('is_alive')]

    print(f"\nResults: {len(alive)} alive, {len(dead)} dead")

    return checked


def dedupe_by_domain_step(leads):
    """Dedupe by domain."""
    print_section("Step 5: Domain Deduplication")

    deduped = dedupe_by_domain(leads, use_final_url=True)
    print(f"Domain dedup: {len(leads)} -> {len(deduped)} leads")

    return deduped


def write_csv_output(leads, region_key, output_dir="output"):
    """Write CSV output."""
    print_section("Step 6: Write CSV")

    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"full_{region_key}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    # Sort by score
    leads.sort(key=lambda x: int(x.get('score', 0)), reverse=True)

    # Write CSV
    with open(filepath, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=FINAL_SCHEMA, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for lead in leads:
            row = {col: lead.get(col, '') for col in FINAL_SCHEMA}
            writer.writerow(row)

    print(f"Wrote {len(leads)} leads to {filepath}")

    # Show top results
    print("\nTop 5 results:")
    for lead in leads[:5]:
        print(f"  {lead.get('score', 0):3} | {lead.get('store_name', 'N/A')[:30]} | {lead.get('url', '')[:40]}")

    return filepath


def push_to_sheets(csv_path, region_key, dry_run=True):
    """Push CSV to Google Sheets."""
    print_section("Step 7: Sheets Push")

    spreadsheet_id = os.environ.get('SHEETS_SPREADSHEET_ID')
    if not spreadsheet_id:
        print("SHEETS_SPREADSHEET_ID not set - skipping sheets push")
        return

    from src.sheets_writer import ensure_sheet_and_header, read_existing_urls, append_rows, DEFAULT_HEADER, _normalize_url

    sheet_name = f"{region_key}_raw"

    # Read CSV
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Read {len(rows)} rows from CSV")

    # Ensure sheet exists
    ensure_sheet_and_header(spreadsheet_id, sheet_name, header=DEFAULT_HEADER)
    print(f"Ensured sheet '{sheet_name}' exists")

    # Read existing URLs
    existing = read_existing_urls(spreadsheet_id, sheet_name)
    print(f"Found {len(existing)} existing URLs in sheet")

    # Prepare rows to append
    to_append = []
    skipped = 0
    for r in rows:
        url = r.get('url', '').strip()
        if not url:
            continue
        norm = _normalize_url(url)
        if norm in existing:
            skipped += 1
            continue
        vals = [r.get(col, '') for col in DEFAULT_HEADER]
        to_append.append(vals)

    print(f"To append: {len(to_append)} rows ({skipped} duplicates skipped)")

    if dry_run:
        print("DRY RUN - not writing to sheets")
    else:
        if to_append:
            resp = append_rows(spreadsheet_id, sheet_name, to_append)
            print(f"Appended {len(to_append)} rows to {sheet_name}")
        else:
            print("Nothing to append")


def main():
    parser = argparse.ArgumentParser(description='Full-scale pipeline')
    parser.add_argument('regions', nargs='+',
                        help='Regions to search (tokyo, kanagawa, saitama)')
    parser.add_argument('--with-sheets', action='store_true', help='Push to Google Sheets')
    parser.add_argument('--commit', action='store_true', help='Actually write (not dry-run)')
    parser.add_argument('--urls-per-query', type=int, default=10, help='URLs per query (default: 10)')
    parser.add_argument('--max-queries', type=int, default=100, help='Max queries per region (default: 100, 0=unlimited)')
    args = parser.parse_args()

    # Region mapping (English -> Japanese)
    REGION_MAP = {
        'tokyo': '東京',
        'kanagawa': '神奈川',
        'saitama': '埼玉',
    }

    # Build regions list from arguments
    regions = []
    for r in args.regions:
        r_lower = r.lower()
        if r_lower in REGION_MAP:
            regions.append((REGION_MAP[r_lower], r_lower))
        else:
            print(f"Warning: Unknown region '{r}', skipping")

    if not regions:
        print("No valid regions specified. Available: tokyo, kanagawa, saitama")
        return

    all_csv_paths = []

    for region_jp, region_key in regions:
        print("\n" + "#" * 60)
        print(f"# FULL SEARCH: {region_jp} ({region_key})")
        print("#" * 60)

        # Run full pipeline
        max_q = args.max_queries if args.max_queries > 0 else None
        urls = run_full_search(region_jp, urls_per_query=args.urls_per_query, max_queries=max_q)

        if not urls:
            print(f"No URLs found for {region_jp} - skipping")
            continue

        leads = process_urls(urls, region_jp)

        if not leads:
            print(f"No valid leads for {region_jp} - skipping")
            continue

        normalized = normalize_and_dedupe(leads, region_jp)
        checked = check_liveness(normalized, max_workers=10)
        deduped = dedupe_by_domain_step(checked)

        # Filter alive only
        alive = [l for l in deduped if l.get('is_alive')]
        print(f"\nFinal alive leads: {len(alive)}")

        csv_path = write_csv_output(alive, region_key)
        all_csv_paths.append((csv_path, region_key))

        if args.with_sheets:
            push_to_sheets(csv_path, region_key, dry_run=not args.commit)

    print_section("FULL PIPELINE COMPLETE")
    print("CSV outputs:")
    for path, key in all_csv_paths:
        print(f"  - {path}")


if __name__ == '__main__':
    main()
