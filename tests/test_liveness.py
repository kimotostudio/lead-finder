"""
Tests for src/liveness.py module.

Run with: python -m pytest tests/test_liveness.py -v
Or standalone: python tests/test_liveness.py
"""
import sys
import os
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.liveness import (
    LivenessChecker,
    check_leads_liveness,
    dedupe_by_domain,
    extract_domain,
    ALIVE_STATUS_CODES,
    DEAD_STATUS_CODES,
)


class MockResponse:
    """Mock HTTP response."""
    def __init__(self, status_code, url=None):
        self.status_code = status_code
        self.url = url or 'https://example.com'

    def close(self):
        pass


def test_alive_status_codes():
    """Test that expected status codes are in ALIVE set."""
    assert 200 in ALIVE_STATUS_CODES
    assert 301 in ALIVE_STATUS_CODES
    assert 302 in ALIVE_STATUS_CODES
    assert 401 in ALIVE_STATUS_CODES  # Protected but exists
    assert 403 in ALIVE_STATUS_CODES  # Forbidden but exists


def test_dead_status_codes():
    """Test that expected status codes are in DEAD set."""
    assert 404 in DEAD_STATUS_CODES
    assert 410 in DEAD_STATUS_CODES


def test_extract_domain_basic():
    """Test domain extraction from URLs."""
    assert extract_domain('https://example.com') == 'example.com'
    assert extract_domain('https://www.example.com') == 'example.com'
    assert extract_domain('http://example.com/path') == 'example.com'
    assert extract_domain('example.com') == 'example.com'


def test_extract_domain_subdomains():
    """Test domain extraction preserves subdomains (except www)."""
    assert extract_domain('https://blog.example.com') == 'blog.example.com'
    assert extract_domain('https://api.example.com/v1') == 'api.example.com'


def test_extract_domain_empty():
    """Test domain extraction handles empty/invalid input."""
    assert extract_domain('') == ''
    assert extract_domain(None) == ''


def test_dedupe_by_domain_keeps_higher_score():
    """Test deduplication keeps lead with higher score."""
    leads = [
        {'url': 'https://example.com/page1', 'store_name': 'Store A', 'score': 50},
        {'url': 'https://example.com/page2', 'store_name': 'Store B', 'score': 80},
        {'url': 'https://other.com/', 'store_name': 'Store C', 'score': 60},
    ]

    deduped = dedupe_by_domain(leads)

    assert len(deduped) == 2  # Two unique domains

    # Find example.com lead
    example_lead = next(l for l in deduped if 'example.com' in l['url'])
    assert example_lead['score'] == 80  # Higher score kept


def test_dedupe_by_domain_uses_final_url():
    """Test deduplication uses final_url when available."""
    leads = [
        {'url': 'https://old.com', 'final_url': 'https://new.com', 'score': 50},
        {'url': 'https://new.com/other', 'final_url': 'https://new.com/other', 'score': 70},
    ]

    deduped = dedupe_by_domain(leads, use_final_url=True)

    assert len(deduped) == 1  # Same domain after redirect
    assert deduped[0]['score'] == 70  # Higher score kept


def test_liveness_checker_200_is_alive():
    """Test that HTTP 200 is treated as alive."""
    checker = LivenessChecker()

    with patch.object(checker.session, 'get') as mock_get:
        mock_get.return_value = MockResponse(200, 'https://example.com')

        result = checker.check_url('https://example.com')

        assert result['is_alive'] is True
        assert result['http_status'] == 200


def test_liveness_checker_403_is_alive():
    """Test that HTTP 403 (Forbidden) is treated as alive."""
    checker = LivenessChecker()

    with patch.object(checker.session, 'get') as mock_get:
        mock_get.return_value = MockResponse(403, 'https://example.com')

        result = checker.check_url('https://example.com')

        assert result['is_alive'] is True
        assert result['http_status'] == 403


def test_liveness_checker_404_is_dead():
    """Test that HTTP 404 is treated as dead."""
    checker = LivenessChecker()

    with patch.object(checker.session, 'get') as mock_get:
        mock_get.return_value = MockResponse(404, 'https://example.com')

        result = checker.check_url('https://example.com')

        assert result['is_alive'] is False
        assert result['http_status'] == 404


def test_liveness_checker_redirect_updates_final_url():
    """Test that redirects update final_url."""
    checker = LivenessChecker()

    with patch.object(checker.session, 'get') as mock_get:
        # Simulate redirect: original URL -> final URL
        mock_get.return_value = MockResponse(200, 'https://new-domain.com/final')

        result = checker.check_url('https://old-domain.com')

        assert result['is_alive'] is True
        assert result['final_url'] == 'https://new-domain.com/final'


def test_liveness_checker_empty_url():
    """Test handling of empty URL."""
    checker = LivenessChecker()

    result = checker.check_url('')

    assert result['is_alive'] is False
    assert result['error'] == 'Empty URL'


def test_liveness_checker_adds_https():
    """Test that missing protocol is handled."""
    checker = LivenessChecker()

    with patch.object(checker.session, 'get') as mock_get:
        mock_get.return_value = MockResponse(200, 'https://example.com')

        result = checker.check_url('example.com')

        # Should have attempted with https://
        call_args = mock_get.call_args[0][0]
        assert call_args.startswith('https://')


def test_check_leads_liveness_filters_dead():
    """Test that dead leads are filtered by default."""
    leads = [
        {'url': 'https://alive.com', 'score': 50},
        {'url': 'https://dead.com', 'score': 60},
    ]

    with patch('src.liveness.LivenessChecker.check_url') as mock_check:
        def side_effect(url):
            is_alive = 'alive' in url
            return {
                'url': url,
                'http_status': 200 if is_alive else 404,
                'final_url': url,
                'is_alive': is_alive,
                'checked_at_iso': '2024-01-01T00:00:00',
                'error': None if is_alive else 'HTTP 404',
            }
        mock_check.side_effect = side_effect

        result = check_leads_liveness(leads, keep_dead=False)

        assert len(result) == 1
        assert result[0]['url'] == 'https://alive.com'


def test_check_leads_liveness_keeps_dead():
    """Test that dead leads are kept when keep_dead=True."""
    leads = [
        {'url': 'https://alive.com', 'score': 50},
        {'url': 'https://dead.com', 'score': 60},
    ]

    with patch('src.liveness.LivenessChecker.check_url') as mock_check:
        def side_effect(url):
            is_alive = 'alive' in url
            return {
                'url': url,
                'http_status': 200 if is_alive else 404,
                'final_url': url,
                'is_alive': is_alive,
                'checked_at_iso': '2024-01-01T00:00:00',
                'error': None if is_alive else 'HTTP 404',
            }
        mock_check.side_effect = side_effect

        result = check_leads_liveness(leads, keep_dead=True)

        assert len(result) == 2


def run_all_tests():
    """Run all tests and report results."""
    tests = [
        ('Alive status codes', test_alive_status_codes),
        ('Dead status codes', test_dead_status_codes),
        ('Extract domain basic', test_extract_domain_basic),
        ('Extract domain subdomains', test_extract_domain_subdomains),
        ('Extract domain empty', test_extract_domain_empty),
        ('Dedupe keeps higher score', test_dedupe_by_domain_keeps_higher_score),
        ('Dedupe uses final_url', test_dedupe_by_domain_uses_final_url),
        ('HTTP 200 is alive', test_liveness_checker_200_is_alive),
        ('HTTP 403 is alive', test_liveness_checker_403_is_alive),
        ('HTTP 404 is dead', test_liveness_checker_404_is_dead),
        ('Redirect updates final_url', test_liveness_checker_redirect_updates_final_url),
        ('Empty URL handling', test_liveness_checker_empty_url),
        ('Adds https protocol', test_liveness_checker_adds_https),
        ('Filter dead leads', test_check_leads_liveness_filters_dead),
        ('Keep dead leads', test_check_leads_liveness_keeps_dead),
    ]

    passed = 0
    failed = 0

    print("=" * 60)
    print("Running liveness.py tests...")
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
