import requests

from src import solo_classifier
from src.solo_classifier import SoloClassifier, analyze_fetch_response


def test_hard_corporate_detection():
    classifier = SoloClassifier(session=requests.Session())
    html = "<html><body>株式会社テスト 代表取締役 会社概要</body></html>"
    result = classifier.classify("https://example.com", html)
    assert result["classification"] == "corporate"
    assert result["solo_score"] == -999
    assert result["detected_corp_terms"]


def test_solo_detection():
    classifier = SoloClassifier(session=requests.Session())
    html = "<html><body>自宅サロン 完全予約制 ひとりで運営</body></html>"
    result = classifier.classify("https://example.com", html)
    assert result["classification"] in ("solo", "small")
    assert result["solo_score"] >= 6


def test_small_detection():
    classifier = SoloClassifier(session=requests.Session())
    html = "<html><body>サロン オーナー 運営者情報</body></html>"
    result = classifier.classify("https://example.com", html)
    assert result["classification"] in ("small", "solo")
    assert result["solo_score"] >= 2


def test_unknown_detection():
    classifier = SoloClassifier(session=requests.Session())
    html = "<html><body>ようこそ</body></html>"
    result = classifier.classify("https://example.com", html)
    # Minimal content may be classified as unknown or small depending on heuristics
    assert result["classification"] in ("unknown", "small")


def test_invalid_url_scheme():
    normalized, error = solo_classifier._normalize_origin_url("ftp://example.com")
    assert normalized is None
    assert error == "INVALID_SCHEME"


def test_non_html_response():
    ok, status, code = analyze_fetch_response(200, "application/pdf", "")
    assert not ok
    assert status == "INVALID"
    assert code == "NON_HTML"


def test_blocked_response():
    ok, status, code = analyze_fetch_response(403, "text/html", "access denied")
    assert not ok
    assert status == "BLOCKED"
    assert code == "BLOCKED_403"


def test_timeout_handling(monkeypatch):
    classifier = SoloClassifier(session=requests.Session())

    def _raise_timeout(*args, **kwargs):
        raise requests.exceptions.Timeout()

    monkeypatch.setattr(solo_classifier, "_fetch_response", _raise_timeout)
    result = classifier.inspect_home("https://example.com")
    assert not result.ok
    assert result.error_code == "TIMEOUT"


def test_dns_fail_handling(monkeypatch):
    classifier = SoloClassifier(session=requests.Session())

    def _raise_conn(*args, **kwargs):
        raise requests.exceptions.ConnectionError()

    monkeypatch.setattr(solo_classifier, "_fetch_response", _raise_conn)
    result = classifier.inspect_home("https://example.com")
    assert not result.ok
    assert result.error_code == "DNS_FAIL"
