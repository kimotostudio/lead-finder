#!/usr/bin/env python3
"""
Convert CSV output to simple 3-line text format for sales team.

Format:
店舗名
URL
判定コメント（△理由を1行）

(空行)
"""
import argparse
import csv
import sys


def generate_comment(lead: dict) -> str:
    """Generate sales-focused comment based on lead data."""
    comments = []

    # Site type
    site_type = lead.get('site_type', '')
    if site_type in ['peraichi', 'crayon', 'jimdo', 'wix', 'ameblo']:
        comments.append(f'{site_type}無料サイト')

    # Missing features
    reasons = lead.get('reasons', '')
    if 'no_pricing' in reasons:
        comments.append('料金不明')
    if 'no_booking' in reasons:
        comments.append('予約導線弱')
    if 'no_access' in reasons:
        comments.append('アクセス情報散在')
    if 'no_profile' in reasons:
        comments.append('プロフィール弱')
    if 'http_only' in reasons:
        comments.append('HTTP非対応')
    if 'sns_redirect' in reasons:
        comments.append('SNS誘導のみ')

    # Score/grade
    score = int(lead.get('score', 0))
    grade = lead.get('grade', 'C')

    if score >= 80:
        comments.append('改善余地大')
    elif score >= 60:
        comments.append('改善提案可')
    elif score >= 40:
        comments.append('一部改善可')
    else:
        comments.append('現状維持傾向')

    # Business type
    business_type = lead.get('business_type', '不明')
    if business_type == '不明':
        comments.append('業種不明確')

    # No phone
    if not lead.get('phone'):
        comments.append('電話番号なし')

    return ' / '.join(comments) if comments else '要確認'


def convert_csv_to_simple(input_path: str, output_path: str, limit: int = None):
    """
    Convert CSV to simple 3-line text format.

    Args:
        input_path: Input CSV path
        output_path: Output text path
        limit: Optional limit on number of leads
    """
    with open(input_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        leads = list(reader)

    if limit:
        leads = leads[:limit]

    with open(output_path, 'w', encoding='utf-8') as f:
        for i, lead in enumerate(leads, 1):
            shop_name = lead.get('shop_name', '名称不明')
            url = lead.get('url', '')
            comment = generate_comment(lead)

            f.write(f"{shop_name}\n")
            f.write(f"{url}\n")
            f.write(f"△ {comment}\n")

            # Add blank line except for last entry
            if i < len(leads):
                f.write("\n")

    print(f"Converted {len(leads)} leads to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Convert CSV to simple 3-line text format'
    )
    parser.add_argument(
        'input',
        help='Input CSV file'
    )
    parser.add_argument(
        '-o', '--output',
        help='Output text file (default: input_simple.txt)'
    )
    parser.add_argument(
        '-l', '--limit',
        type=int,
        help='Limit number of leads'
    )

    args = parser.parse_args()

    # Determine output path
    if args.output:
        output_path = args.output
    else:
        output_path = args.input.replace('.csv', '_simple.txt')

    convert_csv_to_simple(args.input, output_path, args.limit)


if __name__ == '__main__':
    main()
