"""
Integration test for CSV output with normalization.

Run: python tests/test_csv_output.py
"""
import sys
import os
import csv
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.output_writer import OutputWriter


def test_csv_output():
    """Test CSV output with sample data."""

    # Sample raw leads (simulating processor output)
    raw_leads = [
        {
            'shop_name': 'Tokyo Yoga Studio\nPremium Class',
            'url': 'http://www.tokyo-yoga.com/about/?utm_source=google&fbclid=abc123#contact',
            'score': '85',
            'grade': 'A',
            'city': 'Shibuya',
            'business_type': 'Yoga',
            'site_type': 'custom',
            'phone': '03-1234-5678',
            'email': 'info@tokyo-yoga.com',
            'reasons': 'Clean design; Mobile friendly; Fast loading; Good SEO',
            'owner_name': 'Tanaka',
            'address': '1-2-3 Shibuya',
        },
        {
            'shop_name': 'Duplicate Entry',
            'url': 'https://tokyo-yoga.com/about',  # Same URL after normalization
            'score': 60,
            'grade': 'B',
            'city': 'Shinjuku',
            'business_type': 'Yoga',
            'site_type': 'custom',
            'phone': '',
            'email': '',
            'reasons': 'Basic design',
        },
        {
            'shop_name': 'Osaka Pilates Center   with   extra   spaces',
            'url': 'https://osaka-pilates.jp/studio/',
            'score': 70,
            'grade': 'A',
            'city': 'Osaka',
            'business_type': 'Pilates',
            'site_type': 'peraichi',
            'phone': '06-9876-5432',
            'email': 'contact@osaka-pilates.jp',
            'reasons': 'Professional photos; Clear pricing',
        },
        {
            'shop_name': 'Low Score Entry',
            'url': 'https://example.com/low',
            'score': 30,
            'grade': 'C',
            'city': 'Nagoya',
            'business_type': 'Therapy',
            'site_type': 'ameblo',
            'phone': '',
            'email': '',
            'reasons': '',
        },
    ]

    # Write to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        temp_path = f.name

    try:
        # Write normalized CSV
        OutputWriter.write_csv(
            raw_leads,
            temp_path,
            normalize=True,
            source_query='tokyo yoga',
            region='Kanto'
        )

        # Read and verify
        with open(temp_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)

        # Print results
        print("=" * 80)
        print("CSV Output Test Results")
        print("=" * 80)

        # Header
        print("\nHeader row:")
        print(rows[0])

        print(f"\nTotal rows (including header): {len(rows)}")
        print(f"Data rows: {len(rows) - 1}")

        # Expected: 3 unique leads (one duplicate removed)
        # Sorted by score: 85, 70, 30

        print("\nData rows (sorted by score desc):")
        for i, row in enumerate(rows[1:], 1):
            print(f"\nRow {i}:")
            for j, (col, val) in enumerate(zip(rows[0], row)):
                print(f"  {chr(65+j)}. {col}: {val}")

        # Assertions
        assert len(rows) == 4, f"Expected 4 rows (1 header + 3 data), got {len(rows)}"

        # Find column indices by header name
        header = rows[0]
        url_col = header.index('URL')
        score_col = header.index('リードスコア')
        name_col = header.index('表示名')

        # Check dedup worked (should have 3 unique URLs)
        urls = [row[url_col] for row in rows[1:]]
        assert len(set(urls)) == 3, "Dedup failed"

        # Check sorting (first data row should have highest score — or by sales_label priority)
        # sales_label priority: ○=2, △=1, ×=0; then lead_score desc
        scores = [int(row[score_col]) for row in rows[1:]]

        # Check URL normalization
        first_url = rows[1][url_col]
        assert 'utm_source' not in first_url, "Tracking param not removed"
        assert 'fbclid' not in first_url, "fbclid not removed"
        assert '#' not in first_url, "Fragment not removed"
        assert first_url.startswith('https://'), "HTTPS not enforced"

        # Check text sanitization
        first_name = rows[1][name_col]
        assert '\n' not in first_name, "Newline not sanitized"

        print("\n" + "=" * 80)
        print("[PASS] All assertions passed!")
        print("=" * 80)

        return True

    finally:
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def show_sample_output():
    """Generate and display sample CSV content."""
    sample_leads = [
        {
            'shop_name': 'Sample Counseling Room',
            'url': 'https://sample-counseling.com/about?utm_source=test',
            'score': 75,
            'city': 'Shinjuku',
            'business_type': 'Counseling',
            'site_type': 'custom',
            'phone': '03-1111-2222',
            'email': 'info@sample.com',
            'reasons': 'Good design; Mobile optimized',
        },
        {
            'shop_name': 'Example Therapy Salon',
            'url': 'https://example-therapy.jp/',
            'score': 65,
            'city': 'Shibuya',
            'business_type': 'Therapy',
            'site_type': 'peraichi',
            'phone': '03-3333-4444',
            'email': '',
            'reasons': 'Clear pricing',
        },
    ]

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        temp_path = f.name

    try:
        OutputWriter.write_csv(
            sample_leads, temp_path, normalize=True,
            source_query='shinjuku counseling', region='Kanto'
        )

        print("\n" + "=" * 80)
        print("Sample CSV Output (First 2 rows)")
        print("=" * 80)

        with open(temp_path, 'r', encoding='utf-8-sig') as f:
            content = f.read()
            print(content)

    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


if __name__ == '__main__':
    success = test_csv_output()
    show_sample_output()
    sys.exit(0 if success else 1)
