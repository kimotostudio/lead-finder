#!/usr/bin/env python3
"""
Simple pipeline wrapper to run the flow for given regions.

Usage examples:
  # Dry-run/test mode for tokyo and hokkaido
  python tools/pipeline.py tokyo hokkaido

  # Full mode and commit (actually append to sheets)
  python tools/pipeline.py tokyo kanagawa --mode full --commit

Notes:
- The script maps common English region keys to the project's Japanese region keys when calling `advanced_search.py`.
- By default runs in test mode and uses --dry-run when pushing to Google Sheets. Use --commit to actually append.
- Requires GOOGLE_APPLICATION_CREDENTIALS and SHEETS_SPREADSHEET_ID in env if pushing or aggregating.
"""
import argparse
import os
import subprocess
import sys

# Mapping English -> Japanese region keys used by advanced_search.py
ENG_TO_JP = {
    'tokyo': '東京',
    'kanagawa': '神奈川',
    'saitama': '埼玉',
    'hokkaido': '北海道',
    'sapporo': '北海道',
}

# Reverse mapping for sheet tab names (use English lowercased)
JP_TO_ENG = {v: k for k, v in ENG_TO_JP.items()}

PY = sys.executable
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)


def run(cmd, env=None, check=True):
    print('RUN:', ' '.join(cmd))
    subprocess.run(cmd, env=env or os.environ, check=check)


def map_region_for_search(r):
    key = r.strip()
    low = key.lower()
    if low in ENG_TO_JP:
        return ENG_TO_JP[low]
    # if user passed Japanese directly, return as-is
    return key


def sheet_tab_name(r):
    # return english-ish lowercase name for the sheet tab used by push_to_sheets
    low = r.strip().lower()
    # if user passed Japanese, try to map to english
    if low in JP_TO_ENG:
        return JP_TO_ENG[low] + '_raw'
    # map common english terms
    if low in ENG_TO_JP:
        return low + '_raw'
    # fallback: use provided string + _raw
    return low + '_raw'


def output_csv_for_region(mapped_jp, mode):
    # advanced_search.py by default writes to output/advanced_{region}_{mode}.csv
    # Use the japanese mapped region name in filename if non-ascii
    return f"output/advanced_{mapped_jp}_{mode}.csv"


def main():
    p = argparse.ArgumentParser()
    p.add_argument('regions', nargs='+', help='Regions (english or japanese), e.g. tokyo hokkaido kanagawa')
    p.add_argument('--mode', choices=['test','full'], default='test')
    p.add_argument('--commit', action='store_true', help='If set, actually append rows to sheets (no --dry-run)')
    p.add_argument('--spreadsheet-id', help='Optional spreadsheet id to pass to push (or use env SHEETS_SPREADSHEET_ID)')
    p.add_argument('--generate', action='store_true', help='If set, run site-generator to build HTML from Main sheet')
    p.add_argument('--base-url', help='Base URL to write into sheet page_url (used with --generate)')
    args = p.parse_args()

    for r in args.regions:
        mapped_jp = map_region_for_search(r)
        mode = args.mode
        print(f"=== Region: {r} -> search key: {mapped_jp} (mode={mode}) ===")

        # 1) Run advanced_search.py
        out_csv = output_csv_for_region(mapped_jp, mode)
        cmd = [PY, 'advanced_search.py', '--region', mapped_jp, '--output', out_csv]
        if mode == 'test':
            cmd.append('--test')
        # run the search
        run(cmd)

        # 2) Fix header to normalized FINAL_SCHEMA keys
        run([PY, 'tools/fix_header.py', out_csv])

        # 3) Push to sheets (dry-run unless --commit)
        sheet_tab = sheet_tab_name(r)
        push_cmd = [PY, 'tools/push_to_sheets.py', '--region', sheet_tab, '--csv', out_csv]
        if args.spreadsheet_id:
            push_cmd += ['--spreadsheet-id', args.spreadsheet_id]
        if not args.commit:
            push_cmd.append('--dry-run')
        run(push_cmd)

    # 4) Aggregate raw -> Main
    print('=== Aggregating raw sheets into Main ===')
    run([PY, 'tools/aggregate_raw_to_main.py'])

    # 5) Optionally run site-generator to produce HTML and write page_url back to sheet
    if args.generate:
        print('=== Running site-generator to produce HTML from Main ===')
        gen_cmd = [PY, 'site-generator/generate_from_sheets.py']
        # prefer explicit spreadsheet id, else environment
        if args.spreadsheet_id:
            gen_cmd += ['--spreadsheet-id', args.spreadsheet_id]
        else:
            sid = os.environ.get('SHEETS_SPREADSHEET_ID')
            if sid:
                gen_cmd += ['--spreadsheet-id', sid]
        # sheet name and output dirs
        gen_cmd += ['--sheet', 'Main', '--out', 'output', '--images', 'output/images']
        if args.base_url:
            gen_cmd += ['--base-url', args.base_url]
        # only write page_url cells when committing
        if args.commit:
            gen_cmd.append('--commit')
        else:
            gen_cmd.append('--dry-run')
        run(gen_cmd)

    print('Pipeline complete.')


if __name__ == '__main__':
    main()
