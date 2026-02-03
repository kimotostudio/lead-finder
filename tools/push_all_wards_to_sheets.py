#!/usr/bin/env python3
"""
Push all `advanced_*_ward.csv` files in `output/` to the spreadsheet as `<roman>_raw` tabs.

Requires `SHEETS_SPREADSHEET_ID` and `GOOGLE_APPLICATION_CREDENTIALS` to be set in the environment.
"""
import os
import subprocess
import sys
import glob
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

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
    'chiyoda': '千代田区',
    'chuo': '中央区',
    'minato': '港区',
}

JP_TO_ROMAN = {v: k for k, v in ROMAN_TO_JP.items()}


def find_ward_csvs(output_dir='output'):
    p = Path(output_dir)
    return list(p.glob('advanced_*_ward.csv'))


def push(csv_path, region_tab, spreadsheet_id=None):
    cmd = [sys.executable, 'tools/push_to_sheets.py', '--region', region_tab, '--csv', str(csv_path)]
    if spreadsheet_id:
        cmd += ['--spreadsheet-id', spreadsheet_id]
    # commit (no --dry-run)
    print('RUN:', ' '.join(cmd))
    subprocess.run(cmd, check=True)


def main():
    sid = os.environ.get('SHEETS_SPREADSHEET_ID')
    if not sid:
        print('SHEETS_SPREADSHEET_ID not set. Aborting.')
        sys.exit(1)

    csvs = find_ward_csvs('output')
    if not csvs:
        print('No ward CSVs found in output/')
        return

    for csvfile in csvs:
        name = csvfile.name
        # advanced_{city}_ward.csv
        parts = name.split('_')
        if len(parts) < 3:
            print('Skipping unknown file:', name)
            continue
        city_jp = '_'.join(parts[1:-1])  # join in case city contains underscores
        # Ensure CSV header matches FINAL_SCHEMA expected by push_to_sheets
        print('Fixing header for', name)
        subprocess.run([sys.executable, 'tools/fix_header.py', str(csvfile)], check=True)
        # map to roman tab name if possible
        roman = JP_TO_ROMAN.get(city_jp)
        if not roman:
            # fallback: make ascii-friendly by removing non-ascii
            roman = ''.join([c for c in city_jp if ord(c) < 128]).strip().lower() or city_jp
        region_tab = roman + '_raw'
        print(f'Pushing {name} -> sheet tab: {region_tab}')
        try:
            push(csvfile, region_tab, spreadsheet_id=sid)
        except subprocess.CalledProcessError as e:
            print('Error pushing', name, e)

    # After pushing all, run aggregator
    print('\nRunning aggregator: tools/aggregate_raw_to_main.py')
    subprocess.run([sys.executable, 'tools/aggregate_raw_to_main.py'], check=True)


if __name__ == '__main__':
    main()
