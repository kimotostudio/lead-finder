"""
Tests for src/normalize.py module.

Run with: python -m pytest tests/test_normalize.py -v
Or standalone: python tests/test_normalize.py
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.normalize import (
    normalize_url_strict,
    sanitize_text,
    normalize_store_name,
    ensure_int_score,
    deduplicate_leads,
    normalize_leads,
    map_to_final_schema,
)


def test_url_normalization_basic():
    """Test basic URL normalization."""
    # Add https if missing
    assert normalize_url_strict('example.com') == 'https://example.com/'
    assert normalize_url_strict('example.com/page') == 'https://example.com/page'

    # Upgrade http to https
    assert normalize_url_strict('http://example.com') == 'https://example.com/'

    # Remove www
    assert normalize_url_strict('https://www.example.com') == 'https://example.com/'

    # Lowercase domain
    assert normalize_url_strict('https://EXAMPLE.COM/Page') == 'https://example.com/Page'


def test_url_normalization_trailing_slash():
    """Test trailing slash handling."""
    # Root domain keeps slash
    assert normalize_url_strict('https://example.com/') == 'https://example.com/'

    # Non-root paths lose trailing slash
    assert normalize_url_strict('https://example.com/page/') == 'https://example.com/page'
    assert normalize_url_strict('https://example.com/a/b/c/') == 'https://example.com/a/b/c'


def test_url_normalization_fragments():
    """Test fragment removal."""
    assert normalize_url_strict('https://example.com/page#section') == 'https://example.com/page'
    assert normalize_url_strict('https://example.com/#top') == 'https://example.com/'


def test_url_normalization_tracking_params():
    """Test tracking parameter removal."""
    # Remove utm params
    url = 'https://example.com/page?utm_source=google&utm_medium=cpc&name=test'
    normalized = normalize_url_strict(url)
    assert 'utm_source' not in normalized
    assert 'utm_medium' not in normalized
    assert 'name=test' in normalized

    # Remove fbclid
    url = 'https://example.com/page?fbclid=abc123&id=456'
    normalized = normalize_url_strict(url)
    assert 'fbclid' not in normalized
    assert 'id=456' in normalized

    # Remove gclid
    url = 'https://example.com?gclid=xyz&product=shoes'
    normalized = normalize_url_strict(url)
    assert 'gclid' not in normalized
    assert 'product=shoes' in normalized


def test_sanitize_text_newlines():
    """Test newline sanitization."""
    assert sanitize_text('hello\nworld') == 'hello world'
    assert sanitize_text('hello\r\nworld') == 'hello world'
    assert sanitize_text('hello\n\n\nworld') == 'hello world'
    assert sanitize_text('a\tb\tc') == 'a b c'


def test_sanitize_text_spaces():
    """Test space collapsing."""
    assert sanitize_text('hello   world') == 'hello world'
    assert sanitize_text('  hello  world  ') == 'hello world'
    assert sanitize_text('a    b    c') == 'a b c'


def test_sanitize_text_max_length():
    """Test max length truncation."""
    # Create a long text that exceeds 30 chars
    text = 'ABCDEFGHIJ' * 5  # 50 characters
    sanitized = sanitize_text(text, max_length=30)
    assert len(sanitized) == 30
    # Last char should be ellipsis (Unicode U+2026)
    assert ord(sanitized[-1]) == 8230

    # Short text unchanged
    short = 'Short text'
    assert sanitize_text(short, max_length=60) == short


def test_normalize_store_name():
    """Test store name normalization for dedup comparison."""
    # Basic normalization
    assert normalize_store_name('ABC Store') == 'abcstore'
    assert normalize_store_name('abc store') == 'abcstore'

    # Remove symbols
    assert normalize_store_name('ABC・Store') == 'abcstore'
    assert normalize_store_name('ABC-Store') == 'abcstore'
    assert normalize_store_name('【ABC Store】') == 'abcstore'

    # Full-width to half-width numbers
    assert normalize_store_name('Store１２３') == 'store123'


def test_ensure_int_score():
    """Test score integer conversion."""
    assert ensure_int_score(50) == 50
    assert ensure_int_score('75') == 75
    assert ensure_int_score(None) == 0
    assert ensure_int_score('invalid') == 0

    # Clamp to 0-100
    assert ensure_int_score(-10) == 0
    assert ensure_int_score(150) == 100


def test_deduplicate_by_url():
    """Test deduplication by URL keeps higher score."""
    leads = [
        {'url': 'https://example.com/a', 'store_name': 'Store A', 'score': 50, 'city': 'Tokyo'},
        {'url': 'https://example.com/a', 'store_name': 'Store A v2', 'score': 70, 'city': 'Tokyo'},
        {'url': 'https://example.com/b', 'store_name': 'Store B', 'score': 60, 'city': 'Osaka'},
    ]

    deduped = deduplicate_leads(leads)

    assert len(deduped) == 2

    # Find the example.com/a lead
    lead_a = next(l for l in deduped if l['url'] == 'https://example.com/a')
    assert lead_a['score'] == 70  # Higher score kept


def test_deduplicate_by_name_city_fallback():
    """Test deduplication by name+city when URL missing."""
    leads = [
        {'url': '', 'store_name': 'My Store', 'score': 40, 'city': 'Tokyo'},
        {'url': '', 'store_name': 'My Store', 'score': 60, 'city': 'Tokyo'},  # Same name+city, higher score
        {'url': '', 'store_name': 'My Store', 'score': 50, 'city': 'Osaka'},  # Different city
    ]

    deduped = deduplicate_leads(leads)

    assert len(deduped) == 2  # Two unique name+city combos

    # Tokyo lead should have higher score
    tokyo_lead = next(l for l in deduped if l['city'] == 'Tokyo')
    assert tokyo_lead['score'] == 60


def test_normalize_leads_full_pipeline():
    """Test full normalization pipeline."""
    raw_leads = [
        {
            'shop_name': 'Test Shop\nWith Newline',
            'url': 'http://www.example.com/page/?utm_source=google#section',
            'score': '75',
            'city': 'Tokyo',
            'business_type': 'Yoga',
            'site_type': 'custom',
            'phone': '03-1234-5678',
            'email': 'test@example.com',
            'reasons': 'Good design; Mobile friendly',
        },
        {
            'shop_name': 'Duplicate Shop',
            'url': 'https://example.com/page',  # Same as above after normalization
            'score': 50,
            'city': 'Osaka',
            'business_type': 'Pilates',
            'site_type': 'peraichi',
            'phone': '',
            'email': '',
            'reasons': '',
        },
    ]

    normalized = normalize_leads(raw_leads, source_query='tokyo yoga', region='関東')

    # Should dedupe to 1 lead (same normalized URL)
    assert len(normalized) == 1

    lead = normalized[0]

    # Check normalization applied
    assert lead['store_name'] == 'Test Shop With Newline'  # Newline removed
    assert lead['url'] == 'https://example.com/page'  # Normalized
    assert lead['score'] == 75  # Higher score kept
    assert 'utm_source' not in lead['url']
    assert lead['region'] == '関東'
    assert lead['source_query'] == 'tokyo yoga'
    assert 'fetched_at_iso' in lead


def test_map_to_final_schema():
    """Test mapping raw lead to final schema."""
    raw = {
        'shop_name': 'My Shop',
        'url': 'https://example.com',
        'score': 80,
        'city': 'Shibuya',
        'business_type': 'Coaching',
        'site_type': 'custom',
        'phone': '090-1234-5678',
        'email': 'info@example.com',
        'reasons': 'Clean design; Fast loading',
    }

    mapped = map_to_final_schema(raw, source_query='shibuya coaching', region='関東')

    assert mapped['store_name'] == 'My Shop'
    assert mapped['url'] == 'https://example.com/'
    assert mapped['score'] == 80
    assert mapped['city'] == 'Shibuya'
    assert mapped['region'] == '関東'
    assert mapped['source_query'] == 'shibuya coaching'
    assert 'fetched_at_iso' in mapped
    assert mapped['comment']  # Should have comment from reasons


def run_all_tests():
    """Run all tests and report results."""
    tests = [
        ('URL normalization basic', test_url_normalization_basic),
        ('URL trailing slash', test_url_normalization_trailing_slash),
        ('URL fragments', test_url_normalization_fragments),
        ('URL tracking params', test_url_normalization_tracking_params),
        ('Text newline sanitization', test_sanitize_text_newlines),
        ('Text space collapsing', test_sanitize_text_spaces),
        ('Text max length', test_sanitize_text_max_length),
        ('Store name normalization', test_normalize_store_name),
        ('Score integer conversion', test_ensure_int_score),
        ('Dedup by URL', test_deduplicate_by_url),
        ('Dedup by name+city', test_deduplicate_by_name_city_fallback),
        ('Full pipeline', test_normalize_leads_full_pipeline),
        ('Map to schema', test_map_to_final_schema),
    ]

    passed = 0
    failed = 0

    print("=" * 60)
    print("Running normalize.py tests...")
    print("=" * 60)

    for name, test_func in tests:
        try:
            test_func()
            print(f"[PASS] {name}")
            passed += 1
        except AssertionError as e:
            print(f"[FAIL] {name}")
            print(f"  Error: {e}")
            failed += 1
        except Exception as e:
            print(f"[ERROR] {name}")
            print(f"  Exception: {e}")
            failed += 1

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
