#!/usr/bin/env python3
"""CLI to push a cleaned CSV into a Google Spreadsheet region tab idempotently.

Usage:
  python push_to_sheets.py --region tokyo --csv ./output/scraping_tokyo_clean.csv [--spreadsheet-id ID] [--dry-run]

Environment:
  - GOOGLE_APPLICATION_CREDENTIALS must point to service account JSON
  - or pass --spreadsheet-id (or set SHEETS_SPREADSHEET_ID env var)

Behavior:
  - Ensures worksheet exists and header row is present
  - Loads existing normalized URLs from column B and skips duplicates
  - Appends only new rows preserving header order
"""

import os
import csv
import argparse
from src.sheets_writer import ensure_sheet_and_header, read_existing_urls, append_rows, DEFAULT_HEADER, _normalize_url


def read_csv_rows(csv_path):
    rows = []
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return reader.fieldnames, rows


def build_values_for_append(fieldnames, rows):
    # Ensure order matches DEFAULT_HEADER
    header = DEFAULT_HEADER
    values = []
    for r in rows:
        # map to header order
        row_vals = [r.get(col, '') for col in header]
        values.append(row_vals)
    return values


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--region', required=True, help='Worksheet/tab name (e.g., tokyo)')
    p.add_argument('--csv', required=True, help='Path to cleaned CSV file')
    p.add_argument('--spreadsheet-id', help='Spreadsheet ID (or set SHEETS_SPREADSHEET_ID env)')
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    spreadsheet_id = args.spreadsheet_id or os.environ.get('SHEETS_SPREADSHEET_ID')
    if not spreadsheet_id:
        raise SystemExit('Provide --spreadsheet-id or set SHEETS_SPREADSHEET_ID')

    fieldnames, rows = read_csv_rows(args.csv)
    # Normalize incoming URLs and prepare rows
    candidates = []
    for r in rows:
        url = r.get('url','').strip()
        if not url:
            # skip rows without URL by default
            continue
        norm = _normalize_url(url)
        if not norm:
            continue
        r['_normalized_url'] = norm
        candidates.append(r)

    # ensure sheet/header
    ensure_sheet_and_header(spreadsheet_id, args.region, header=DEFAULT_HEADER)

    existing = read_existing_urls(spreadsheet_id, args.region)

    to_append = []
    skipped = 0
    for r in candidates:
        if r['_normalized_url'] in existing:
            skipped += 1
            continue
        # build row values in DEFAULT_HEADER order
        vals = [r.get(col,'') for col in DEFAULT_HEADER]
        to_append.append(vals)

    print(f'Found {len(candidates)} candidate rows; {skipped} duplicates skipped; {len(to_append)} to append.')

    if args.dry_run:
        print('Dry run: not writing. Sample to append:')
        for v in to_append[:5]:
            print(v)
        return

    if to_append:
        resp = append_rows(spreadsheet_id, args.region, to_append)
        print('Append response:', resp.get('updates', {}))
    else:
        print('Nothing to append.')


if __name__ == '__main__':
    main()
