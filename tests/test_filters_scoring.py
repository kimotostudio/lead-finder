import pytest

from src.filters import get_filter_reason, is_relevant_lead
from src.scoring_rules import boost_score, apply_scoring_boost


def make_lead(url='', title='', visible_text='', score=10, site_type=''):
    return {
        'url': url,
        'shop_name': title,
        'title': title,
        'visible_text': visible_text,
        'reasons': '',
        'score': score,
        'site_type': site_type,
    }


def test_hard_excluded_domains():
    # ameblo should be excluded
    lead = make_lead(url='https://ameblo.jp/someblog', title='個人のブログ')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert 'ameblo.jp' in reason

    # note.com excluded
    lead = make_lead(url='https://note.com/author/n', title='note記事')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert 'note.com' in reason

    # hotpepper excluded
    lead = make_lead(url='https://www.hotpepper.jp/xxxx', title='ホットペッパー')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert 'hotpepper' in reason


def test_medical_institution_excluded():
    """Medical institution pages should be excluded."""
    lead = make_lead(url='https://example.com/clinic', title='〇〇クリニック - 診療案内')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert 'medical' in reason


def test_counseling_kept():
    lead = make_lead(url='https://private-counseling.example', title='個人カウンセリングルーム', visible_text='完全予約制、個人セッション')
    is_filtered, reason = get_filter_reason(lead)
    assert not is_filtered
    assert reason == ''


def test_boost_for_reservation_only():
    # A counseling lead with 完全予約制 should get a positive boost
    lead = make_lead(url='https://example.com', title='カウンセリングルーム', visible_text='完全予約制、個人セッション', score=20, site_type='jimdo')
    boost, reasons = boost_score(lead)
    assert boost > 0

    # Applying full boost should increase the stored score
    leads = [lead.copy()]
    updated = apply_scoring_boost(leads)
    assert updated[0]['score'] >= 20


def test_additional_blocked_domains_and_keywords():
    # beauty.rakuten (explicit subdomain)
    lead = make_lead(url='https://beauty.rakuten.co.jp/salon/123', title='楽天ビューティ掲載')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert 'rakuten' in reason or 'blocked_keyword' in reason

    # instagram should be blocked
    lead = make_lead(url='https://instagram.com/someuser', title='Instagram')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert 'instagram.com' in reason

    # google maps URL should be blocked
    lead = make_lead(url='https://www.google.com/maps/place/SomePlace', title='地図')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert 'google_maps' in reason


def test_self_hosted_not_filtered():
    # Normal self-hosted domain with counseling keywords should not be filtered
    lead = make_lead(url='https://my-private-clinic.example', title='個人カウンセリングルーム', visible_text='完全予約制、カウンセリング')
    is_filtered, reason = get_filter_reason(lead)
    assert not is_filtered
    assert reason == ''
