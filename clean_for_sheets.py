#!/usr/bin/env python3
"""
Clean and format CSV output for Google Sheets compatibility.

Features:
- Optimized column order (important info on left)
- UTF-8 with BOM encoding (Excel/Sheets compatible)
- Phone number normalization
- URL validation
- Data cleaning and sorting
- Metadata addition (timestamp, region)
"""
import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


def normalize_phone(phone: str) -> str:
    """
    Normalize phone number to Japanese format with hyphens.

    Examples:
        0312345678 -> 03-1234-5678
        09012345678 -> 090-1234-5678
        03-1234-5678 -> 03-1234-5678 (already formatted)
    """
    if not phone:
        return ''

    # Remove all non-digits
    digits = re.sub(r'\D', '', phone)

    # Skip obviously invalid phones
    if len(digits) < 10:
        return ''

    # Filter out clearly invalid patterns (all zeros, repeating, etc.)
    if digits == '0' * len(digits):
        return ''
    if len(set(digits[:4])) == 1:  # First 4 digits all same
        return ''

    # Truncate if too long (take first 11 digits)
    if len(digits) > 11:
        digits = digits[:11]

    # Format based on length and prefix
    if len(digits) == 10:
        # Landline: 03-1234-5678 or 06-1234-5678
        if digits.startswith(('03', '04', '06')):
            return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
        # Other area codes: 0120-123-456, 050-1234-5678
        elif digits.startswith(('012', '050')):
            return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
        # Standard: 0XX-XXX-XXXX
        else:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11:
        # Mobile: 090-1234-5678
        if digits.startswith(('070', '080', '090')):
            return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
        # Some landlines: 0123-45-6789
        else:
            return f"{digits[:4]}-{digits[4:6]}-{digits[6:]}"

    # Return empty if can't normalize properly
    return ''


def validate_url(url: str) -> bool:
    """Validate URL (must be https and valid format)."""
    if not url:
        return False

    # Allow http for now (many small businesses still use http)
    if not (url.startswith('http://') or url.startswith('https://')):
        return False

    # Basic URL validation
    if len(url) < 10:
        return False

    return True


def generate_improvement_points(lead: Dict) -> str:
    """
    Generate improvement points summary.

    Converts semicolon-separated reasons to comma-separated Japanese points.
    """
    points = []

    # Site type
    site_type = lead.get('site_type', '')
    if site_type in ['peraichi', 'crayon', 'jimdo', 'wix', 'ameblo']:
        points.append(f'{site_type}無料サイト')

    # Parse reasons
    reasons = lead.get('reasons', '')
    if 'no_pricing' in reasons:
        points.append('料金不明')
    if 'no_booking' in reasons:
        points.append('予約導線弱')
    if 'no_access' in reasons:
        points.append('アクセス情報散在')
    if 'no_profile' in reasons:
        points.append('プロフィール弱')
    if 'http_only' in reasons:
        points.append('HTTP非対応')
    if 'sns_redirect' in reasons:
        points.append('SNS誘導のみ')

    # Business type unknown
    if lead.get('business_type') == '不明':
        points.append('業種不明確')

    return ', '.join(points) if points else '要確認'


def clean_csv(
    input_path: str,
    output_clean: str,
    output_raw: Optional[str] = None,
    region: Optional[str] = None,
    query: Optional[str] = None
):
    """
    Clean and format CSV for Google Sheets.

    Args:
        input_path: Input CSV file
        output_clean: Output cleaned CSV file
        output_raw: Optional backup of raw data
        region: Search region (for metadata)
        query: Search query (for metadata)
    """
    # Read input CSV
    with open(input_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        leads = list(reader)

    print(f"Read {len(leads)} leads from {input_path}")

    # Backup raw data if requested
    if output_raw:
        with open(output_raw, 'w', encoding='utf-8-sig', newline='') as f:
            if leads:
                writer = csv.DictWriter(f, fieldnames=leads[0].keys())
                writer.writeheader()
                writer.writerows(leads)
        print(f"Backed up raw data to {output_raw}")

    # Clean and transform data
    cleaned_leads = []
    skipped = 0

    for lead in leads:
        # Validate URL
        url = lead.get('url', '')
        if not validate_url(url):
            skipped += 1
            continue

        # Normalize phone
        phone = normalize_phone(lead.get('phone', ''))

        # Generate improvement points
        improvement_points = generate_improvement_points(lead)

        # Get current timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')

        # Create cleaned lead with optimized column order
        cleaned_lead = {
            '店舗名': lead.get('shop_name', '名称不明'),
            'URL': url,
            '電話番号': phone,
            '業種': lead.get('business_type', '不明'),
            '地域': lead.get('city', region or ''),
            'スコア': lead.get('score', '0'),
            '評価': lead.get('grade', 'C'),
            '改善ポイント': improvement_points,
            'サイトタイプ': lead.get('site_type', 'custom'),
            'メールアドレス': lead.get('email', ''),
            '住所': lead.get('address', ''),
            '営業時間': lead.get('business_hours', ''),
            '最終更新': timestamp,
            '検索地域': region or '',
            '検索クエリ': query or '',
        }

        cleaned_leads.append(cleaned_lead)

    print(f"Cleaned {len(cleaned_leads)} leads")
    print(f"Skipped {skipped} leads (invalid URLs)")

    # Sort by score (descending), then by business type
    cleaned_leads.sort(
        key=lambda x: (
            -int(x['スコア']) if x['スコア'].isdigit() else 0,
            x['業種']
        )
    )

    print("Sorted by score (descending) and business type")

    # Write cleaned CSV with UTF-8 BOM (Excel/Sheets compatible)
    with open(output_clean, 'w', encoding='utf-8-sig', newline='') as f:
        if cleaned_leads:
            # Use QUOTE_NONNUMERIC to ensure proper quoting
            writer = csv.DictWriter(
                f,
                fieldnames=cleaned_leads[0].keys(),
                quoting=csv.QUOTE_MINIMAL
            )
            writer.writeheader()
            writer.writerows(cleaned_leads)

    print(f"Wrote cleaned CSV to {output_clean}")
    print(f"  Encoding: UTF-8 with BOM")
    print(f"  Line endings: CRLF (Windows compatible)")
    print(f"  Total rows: {len(cleaned_leads)}")

    # Print summary statistics
    print("\nSummary:")
    print(f"  Grade A (60+): {sum(1 for x in cleaned_leads if int(x['スコア']) >= 60)}")
    print(f"  Grade B (40-59): {sum(1 for x in cleaned_leads if 40 <= int(x['スコア']) < 60)}")
    print(f"  Grade C (<40): {sum(1 for x in cleaned_leads if int(x['スコア']) < 40)}")

    # Business type breakdown
    business_types = {}
    for lead in cleaned_leads:
        btype = lead['業種']
        business_types[btype] = business_types.get(btype, 0) + 1

    print("\nBusiness types:")
    for btype, count in sorted(business_types.items(), key=lambda x: -x[1]):
        print(f"  {btype}: {count}")

    # Phone coverage
    phones_found = sum(1 for x in cleaned_leads if x['電話番号'])
    print(f"\nContact info:")
    print(f"  Phone numbers: {phones_found}/{len(cleaned_leads)} ({phones_found*100//len(cleaned_leads) if cleaned_leads else 0}%)")


def main():
    parser = argparse.ArgumentParser(
        description='Clean CSV output for Google Sheets compatibility'
    )
    parser.add_argument(
        'input',
        help='Input CSV file'
    )
    parser.add_argument(
        '-o', '--output',
        help='Output cleaned CSV file (default: input_clean.csv)'
    )
    parser.add_argument(
        '--raw-backup',
        help='Backup raw data to this file (default: input_raw.csv)'
    )
    parser.add_argument(
        '--region',
        help='Search region (for metadata)'
    )
    parser.add_argument(
        '--query',
        help='Search query (for metadata)'
    )

    args = parser.parse_args()

    # Determine output paths
    input_path = Path(args.input)

    if args.output:
        output_clean = args.output
    else:
        output_clean = str(input_path.parent / f"{input_path.stem}_clean.csv")

    if args.raw_backup:
        output_raw = args.raw_backup
    else:
        output_raw = str(input_path.parent / f"{input_path.stem}_raw.csv")

    # Clean CSV
    clean_csv(
        str(input_path),
        output_clean,
        output_raw,
        args.region,
        args.query
    )


if __name__ == '__main__':
    main()
