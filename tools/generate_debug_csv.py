"""Generate a small debug CSV to verify filter_reason and boost fields are written."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.output_writer import OutputWriter


def main():
    leads = [
        {
            'shop_name': 'Ameblo Blog Owner',
            'url': 'https://ameblo.jp/someblog',
            'score': 10,
            'reasons': 'blog only',
            'filter_reason': 'excluded_domain:ameblo.jp',
            'score_boost': 0,
            'boost_reasons': '',
            'site_type': 'ameblo',
            'city': '新宿区',
        },
        {
            'shop_name': 'Private Counseling Room',
            'url': 'https://private-counseling.example',
            'score': 65,
            'reasons': '完全予約制、個人セッション',
            'filter_reason': '',
            'score_boost': 25,
            'boost_reasons': '+完全予約制,+個人セッション',
            'site_type': 'jimdo',
            'city': '渋谷区',
        }
    ]

    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'web_app', 'output')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, 'debug_leads.csv')

    OutputWriter.write_csv(leads, out_path, normalize=True, source_query='debug', region='関東')

    # Print first 20 lines
    with open(out_path, 'r', encoding='utf-8-sig') as f:
        for i, line in enumerate(f):
            print(line.rstrip('\n'))
            if i >= 20:
                break


if __name__ == '__main__':
    main()
