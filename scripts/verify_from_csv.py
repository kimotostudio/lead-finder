import sys
from pathlib import Path
import csv
import time

proj_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(proj_root))

from src.processor import LeadProcessor


def detect_url_col(header):
    # possible header names mapping to 'url'
    candidates = ['url', '元のURL', 'website', 'サイト', 'リンク', 'リンク先', 'URL']
    for i, h in enumerate(header):
        if h in candidates:
            return i
        # fuzzy match
        low = h.lower()
        if 'url' in low or 'http' in low or '元の' in h:
            return i
    return None


def read_urls(csv_path, max_urls=20):
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return []
        url_col = detect_url_col(header)
        if url_col is None:
            # try known English header names
            for i, h in enumerate(header):
                if h.strip().lower() in ('url', 'website', 'link'):
                    url_col = i
                    break
        urls = []
        for row in reader:
            if len(row) <= (url_col or 0):
                continue
            if url_col is None:
                # try to find http in row
                found = None
                for cell in row:
                    if cell and cell.startswith('http'):
                        found = cell
                        break
                if found:
                    urls.append(found)
            else:
                val = row[url_col].strip()
                if val:
                    urls.append(val)
            if len(urls) >= max_urls:
                break
    return urls


def main():
    if len(sys.argv) < 2:
        print('Usage: verify_from_csv.py <csv_path> [max_urls]')
        sys.exit(2)
    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        print('File not found:', csv_path)
        sys.exit(1)
    max_urls = int(sys.argv[2]) if len(sys.argv) > 2 else 20

    urls = read_urls(csv_path, max_urls=max_urls)
    if not urls:
        print('No URLs found in CSV')
        sys.exit(1)

    print(f'Found {len(urls)} URLs, processing...')
    proc = LeadProcessor(parallel_workers=4)
    start = time.time()
    final_leads, failed, filtered = proc.process_pipeline(urls)
    took = time.time() - start
    print(f'Processing finished in {took:.1f}s')

    for lead in final_leads:
        print('---')
        print('shop_name:', lead.get('shop_name'))
        print('url:', lead.get('url'))
        print('score:', lead.get('score'))
        print('grade:', lead.get('grade'))
        print('weakness_score:', lead.get('weakness_score', 'N/A'))
        print('reasons:', lead.get('reasons'))
        print('site_type:', lead.get('site_type'))
    if failed:
        print('\nFailed URLs:')
        for u in failed:
            print('-', u)

if __name__ == '__main__':
    main()
