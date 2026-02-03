"""Run multilayer search per city using repository search and processor.

This script implements the user's multilayer query plan (A/B/C) and runs
search + processing for specified prefectures/cities. It is intended to be
run locally where the repo's search engines can access the web.

Usage (project root):
  python tools\run_multilayer_search.py --prefectures Tokyo Kanagawa Saitama --limit 10 --min-score 40

Notes:
- Respects the existing pipeline: crawl -> normalize -> dedupe -> filter -> boost
- Writes CSVs to web_app/output/<prefecture>_<city>_<timestamp>.csv
- This script WILL perform live web searches using `MultiEngineSearch`.
  Run it locally where you have network access and understand rate limits.
"""
import time
import argparse
import os
import sys
from datetime import datetime

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.processor import LeadProcessor
from src.output_writer import OutputWriter
from src.engines.multi_engine import MultiEngineSearch


DEFAULT_CITY_MAP = {
    'Tokyo': ['新宿区', '渋谷区', '世田谷区', '練馬区', '杉並区', '港区'],
    'Kanagawa': ['横浜市', '川崎市', '相模原市', '藤沢市', '茅ヶ崎市', '横須賀市'],
    'Saitama': ['さいたま市', '川口市', '越谷市', '所沢市', '春日部市', '川越市'],
}


def generate_queries_for_city(city: str):
    """Generate layered queries (A + B templates) for a given city."""
    a_templates = [
        f"{city} 相談室 個人",
        f"{city} 対話 セッション",
        f"{city} 個人セッション 自宅",
        f"{city} 心の整理 完全予約制",
        f"{city} 傾聴 カウンセリング 個人",
        f"{city} セラピー 個人サイト",
        f"{city} の部屋 相談",
        f"{city} 庵 相談",
    ]

    b_templates = [
        f"{city} 対話 site:note.com",
        f"{city} 個人セッション site:note.com",
        f"{city} 相談 site:fc2.com",
        f"{city} 相談 site:crayonsite.info",
        f"{city} 相談 site:jimdofree.com",
        f"{city} 相談 site:peraichi.com",
    ]

    return a_templates + b_templates


def generate_layer_c_queries(city: str):
    """Layer C: catch-all patterns for屋号っぽい語尾"""
    c_templates = [
        f'{city} "ルーム" 相談',
        f'{city} "サロン" カウンセリング',
        f'{city} "の部屋" 相談',
        f'{city} "庵" 相談',
        f'{city} "アトリエ" セッション',
        f'{city} "小さなサロン" 相談',
        f'{city} "自宅サロン" セッション',
    ]
    return c_templates


def run_for_prefecture(prefecture: str, cities: list, limit: int, min_score: int, delay: float, out_dir: str):
    searcher = MultiEngineSearch()
    processor = LeadProcessor(parallel_workers=5)

    os.makedirs(out_dir, exist_ok=True)

    for city in cities:
        print(f"\n=== Processing {prefecture} - {city} ===")

        queries = generate_queries_for_city(city)
        # If user requested deeper catch-all, append Layer C
        if run_options.get('use_layer_c'):
            queries += generate_layer_c_queries(city)

        # Dry-run mode: use canned sample URLs to exercise pipeline quickly
        if run_options.get('dry_run'):
            print("Dry-run: using sample URLs (no live searches)")
            collected_urls = [
                'https://example.com/sample-therapy',
                'https://ameblo.jp/sample-blog',
                'https://clinic.example.jp/service',
                'https://note.com/sample/n/n12345',
                'https://selfhosted.example/salon',
            ]
        else:
            collected_urls = []
            seen = set()

            for q in queries:
                try:
                    urls = searcher.search(q, max_results_per_engine=limit)
                except Exception as e:
                    print(f"Search error for query '{q}': {e}")
                    urls = []

                for u in urls:
                    if u in seen:
                        continue
                    seen.add(u)
                    collected_urls.append(u)

                # polite delay
                time.sleep(delay)

        print(f"Collected {len(collected_urls)} unique URLs for {city}")

        if not collected_urls:
            print("No URLs collected; continuing to next city")
            continue

        # Process the collected URLs through the repo pipeline
        leads, failed = processor.process_urls(collected_urls)

        # Deduplicate and apply filters/boosts using existing processor methods
        unique = processor.deduplicate_leads(leads)
        kept, filtered = processor.filter_and_boost(unique)

        # Apply min_score threshold
        final = [l for l in kept if int(l.get('score', 0)) >= min_score]

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"leads_{prefecture}_{city}_{timestamp}.csv"
        path = os.path.join(out_dir, filename)

        OutputWriter.write_csv(final, path, normalize=True, source_query=','.join(queries[:3]), region=prefecture)

        print(f"Wrote {len(final)} leads to {path} (filtered out {len(filtered)})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prefectures', nargs='+', default=['Tokyo', 'Kanagawa', 'Saitama'])
    parser.add_argument('--limit', type=int, default=10)
    parser.add_argument('--min-score', type=int, default=40)
    parser.add_argument('--delay', type=float, default=1.0)
    parser.add_argument('--out-dir', default=os.path.join('web_app', 'output'))
    parser.add_argument('--use-layer-c', action='store_true', help='Enable Layer C catch-all queries')
    parser.add_argument('--cities', nargs='*', help='Optional custom city list to override defaults for all prefectures')
    parser.add_argument('--dry-run', action='store_true', help='Run without performing live searches; use sample URLs')

    args = parser.parse_args()

    # Build run options accessible in run_for_prefecture
    global run_options
    run_options = {
        'use_layer_c': args.use_layer_c,
        'dry_run': args.dry_run,
    }

    for pref in args.prefectures:
        if args.cities:
            cities = args.cities
        else:
            cities = DEFAULT_CITY_MAP.get(pref, [pref])
        run_for_prefecture(pref, cities, args.limit, args.min_score, args.delay, args.out_dir)


if __name__ == '__main__':
    main()
