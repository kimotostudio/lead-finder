#!/usr/bin/env python3
"""
Small pipeline test script - runs a minimal test for Tokyo and Kanagawa.
Tests: search -> normalize -> liveness -> CSV output -> (optional) sheets -> (optional) HTML

Usage:
  # Basic test (search + process + CSV only)
  python tools/test_pipeline_small.py

  # Also test sheets push (dry-run by default)
  python tools/test_pipeline_small.py --with-sheets

  # Also generate HTML from CSV
  python tools/test_pipeline_small.py --with-html

  # Full test with actual writes
  python tools/test_pipeline_small.py --with-sheets --with-html --commit

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

from config.advanced_queries import generate_test_queries, TARGET_REGIONS
from src.engines.multi_engine import MultiEngineSearch
from src.processor import LeadProcessor
from src.output_writer import OutputWriter
from src.normalize import normalize_leads, FINAL_SCHEMA
from src.liveness import check_leads_liveness, dedupe_by_domain


def print_section(title):
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def run_small_search(region_jp, cities_limit=2, queries_per_city=3, urls_per_query=5):
    """Run a small search for the given region."""
    print_section(f"Step 1: Search for {region_jp}")

    queries = generate_test_queries(
        region_jp,
        cities_limit=cities_limit,
        queries_per_city=queries_per_city
    )
    print(f"Generated {len(queries)} queries")
    for q in queries[:5]:
        print(f"  - {q}")
    if len(queries) > 5:
        print(f"  ... and {len(queries) - 5} more")

    # Run search
    searcher = MultiEngineSearch()
    all_urls = []

    for i, query in enumerate(queries):
        print(f"  [{i+1}/{len(queries)}] Searching: {query[:50]}...")
        try:
            urls = searcher.search(query, max_results_per_engine=urls_per_query)
            all_urls.extend(urls)
            print(f"    Found {len(urls)} URLs")
        except Exception as e:
            print(f"    Error: {e}")

    # Dedupe URLs
    unique_urls = list(set(all_urls))
    print(f"\nTotal URLs: {len(all_urls)} -> {len(unique_urls)} unique")

    return unique_urls


def process_urls(urls, region_jp):
    """Process URLs through the lead processor."""
    print_section("Step 2: Process URLs")

    processor = LeadProcessor()
    leads = []

    for i, url in enumerate(urls):
        print(f"  [{i+1}/{len(urls)}] Processing: {url[:60]}...")
        try:
            result = processor.process_url(url)
            if result and result.get('score', 0) > 0:
                result['region'] = region_jp
                leads.append(result)
                print(f"    Score: {result.get('score', 0)}")
        except Exception as e:
            print(f"    Error: {e}")

    print(f"\nProcessed {len(leads)} valid leads from {len(urls)} URLs")
    return leads


def normalize_and_dedupe(leads, region_jp, source_query="test"):
    """Normalize and dedupe leads."""
    print_section("Step 3: Normalize & Dedupe")

    normalized = normalize_leads(leads, source_query=source_query, region=region_jp)
    print(f"Normalized: {len(leads)} -> {len(normalized)} leads")

    # Show sample
    if normalized:
        print("\nSample normalized lead:")
        for k, v in list(normalized[0].items())[:8]:
            print(f"  {k}: {v}")

    return normalized


def check_liveness(leads, max_workers=5):
    """Check URL liveness."""
    print_section("Step 4: Liveness Check")

    print(f"Checking {len(leads)} URLs...")

    checked = check_leads_liveness(
        leads,
        keep_dead=True,
        max_workers=max_workers,
        progress_callback=lambda c, t, u, r: print(f"  [{c}/{t}] {u[:50]}... {'ALIVE' if r['is_alive'] else 'DEAD'}")
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
    filename = f"test_{region_key}_{timestamp}.csv"
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

    # Show sample
    print("\nSample output (top 3):")
    for lead in leads[:3]:
        print(f"  {lead.get('store_name', 'N/A')[:30]} | {lead.get('url', '')[:40]} | Score: {lead.get('score', 0)}")

    return filepath


def test_sheets_push(csv_path, region_key, dry_run=True):
    """Test sheets push."""
    print_section("Step 7: Sheets Push (Test)")

    spreadsheet_id = os.environ.get('SHEETS_SPREADSHEET_ID')
    if not spreadsheet_id:
        print("SHEETS_SPREADSHEET_ID not set - skipping sheets test")
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
        for v in to_append[:3]:
            print(f"  {v[:3]}...")
    else:
        if to_append:
            resp = append_rows(spreadsheet_id, sheet_name, to_append)
            print(f"Appended {len(to_append)} rows")
        else:
            print("Nothing to append")


def generate_html_from_csv(csv_paths, dry_run=True):
    """Generate HTML from CSV files (no sheets required)."""
    print_section("Step 8: HTML Generation from CSV")

    site_gen_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'site-generator')

    # Merge all CSVs and add IDs
    all_rows = []
    for csv_path, region_key in csv_paths:
        with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            all_rows.extend(rows)

    if not all_rows:
        print("No rows to generate HTML for")
        return

    # Assign IDs starting from 03000
    for i, row in enumerate(all_rows):
        row['id'] = str(3000 + i).zfill(5)

    # Write merged CSV with IDs
    merged_csv = os.path.join('output', 'test_main_merged.csv')
    fieldnames = ['id'] + list(all_rows[0].keys())
    fieldnames = list(dict.fromkeys(fieldnames))  # Remove duplicates

    with open(merged_csv, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f"Merged {len(all_rows)} rows into {merged_csv}")
    print(f"IDs: 03000 - {str(3000 + len(all_rows) - 1).zfill(5)}")

    if dry_run:
        print("\nDRY RUN - would generate HTML for:")
        for row in all_rows[:5]:
            print(f"  {row['id']}: {row.get('store_name', 'N/A')[:40]}")
        return

    # Run actual generation
    import subprocess
    cmd = [
        sys.executable,
        os.path.join(site_gen_dir, 'generate_from_sheets.py'),
        '--csv-fallback', merged_csv,
        '--out', 'output',
        '--images', 'output/images'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)


def test_html_generation(dry_run=True):
    """Test HTML generation from Main sheet (requires Google Sheets credentials)."""
    print_section("Step 8: HTML Generation from Sheets")

    spreadsheet_id = os.environ.get('SHEETS_SPREADSHEET_ID')
    if not spreadsheet_id:
        print("SHEETS_SPREADSHEET_ID not set - skipping sheets HTML test")
        return

    # Change to site-generator dir
    site_gen_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'site-generator')
    sys.path.insert(0, site_gen_dir)

    from sheet_reader import read_sheet_rows

    rows = read_sheet_rows(spreadsheet_id, 'Main')
    print(f"Read {len(rows)} rows from Main sheet")

    if not rows:
        print("No rows in Main sheet - skipping HTML generation")
        return

    # Count rows with valid IDs
    valid = [r for r in rows if r.get('id') or r.get('ID')]
    print(f"Rows with valid IDs: {len(valid)}")

    if dry_run:
        print("DRY RUN - would generate HTML for:")
        for r in valid[:5]:
            id_val = r.get('id') or r.get('ID')
            name = r.get('store_name', 'N/A')
            print(f"  {id_val}: {name[:40]}")
    else:
        # Run actual generation
        import subprocess
        cmd = [
            sys.executable,
            os.path.join(site_gen_dir, 'generate_from_sheets.py'),
            '--spreadsheet-id', spreadsheet_id,
            '--sheet', 'Main',
            '--out', 'output',
            '--images', 'output/images'
        ]
        subprocess.run(cmd)


def main():
    parser = argparse.ArgumentParser(description='Small pipeline test')
    parser.add_argument('regions', nargs='*', default=['tokyo', 'kanagawa'],
                        help='Regions to test (e.g., tokyo kanagawa saitama). Default: tokyo kanagawa')
    parser.add_argument('--with-sheets', action='store_true', help='Also test sheets push')
    parser.add_argument('--with-html', action='store_true', help='Also test HTML generation')
    parser.add_argument('--commit', action='store_true', help='Actually write to sheets (not dry-run)')
    parser.add_argument('--skip-search', action='store_true', help='Skip search, use existing CSV')
    parser.add_argument('--csv', help='Use existing CSV instead of searching')
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
        print(f"# TESTING REGION: {region_jp} ({region_key})")
        print("#" * 60)

        if args.csv:
            csv_path = args.csv
            print(f"Using provided CSV: {csv_path}")
        elif args.skip_search:
            print("Skipping search - need --csv to specify input")
            continue
        else:
            # Run full pipeline for this region
            urls = run_small_search(region_jp, cities_limit=2, queries_per_city=2, urls_per_query=5)

            if not urls:
                print(f"No URLs found for {region_jp} - skipping")
                continue

            leads = process_urls(urls[:20], region_jp)  # Limit to 20 URLs for speed

            if not leads:
                print(f"No valid leads for {region_jp} - skipping")
                continue

            normalized = normalize_and_dedupe(leads, region_jp)
            checked = check_liveness(normalized, max_workers=5)
            deduped = dedupe_by_domain_step(checked)

            # Filter alive only
            alive = [l for l in deduped if l.get('is_alive')]
            print(f"\nFinal alive leads: {len(alive)}")

            csv_path = write_csv_output(alive, region_key)

        all_csv_paths.append((csv_path, region_key))

        if args.with_sheets:
            test_sheets_push(csv_path, region_key, dry_run=not args.commit)

    if args.with_html:
        # Generate HTML from CSV (doesn't require sheets)
        generate_html_from_csv(all_csv_paths, dry_run=not args.commit)

    print_section("PIPELINE TEST COMPLETE")
    print("CSV outputs:")
    for path, key in all_csv_paths:
        print(f"  - {path}")

    if args.with_html and args.commit:
        print("\nHTML outputs:")
        print("  - output/html/*.html")


if __name__ == '__main__':
    main()
