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


@pytest.mark.parametrize(
    "url,title",
    [
        ("https://small-salon.jimdofree.com/", "福岡の小さな個人サロン 掲載ページ"),
        ("https://owner-salon.jimdo.com/", "福岡 プライベートサロン"),
        ("https://owner-studio.wixsite.com/fukuoka", "福岡 ヨガスタジオ"),
        ("https://peraichi.com/landing_pages/view/fukuoka-salon", "福岡 ペライチ 個人サロン"),
        ("https://studio.site/fukuoka-studio", "福岡 小規模スタジオ"),
        ("https://owner-salon.amebaownd.com/", "福岡 Ameba Ownd サロン"),
        ("https://small-shop.stores.jp/", "福岡 予約制サロン"),
        ("https://owner-salon.base.shop/", "福岡 プライベートサロン"),
        ("https://small-salon.goope.jp/", "福岡 小さなサロン"),
        ("https://crayon.e-shops.jp/salon/", "福岡 個人サロン"),
    ],
)
def test_simple_builder_small_business_pages_not_hard_excluded(url, title):
    lead = make_lead(
        url=url,
        title=title,
        visible_text="完全予約制の小さなサロンです。お問い合わせフォームからご相談ください。",
    )
    is_filtered, reason = get_filter_reason(lead)
    assert not is_filtered
    assert reason == ''


def test_simple_builder_real_portal_language_still_excluded():
    lead = make_lead(
        url='https://studio.site/fukuoka-ranking',
        title='福岡サロンおすすめランキング20選',
        visible_text='比較と口コミをまとめた情報サイトです。',
    )
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert reason in {'aggregator_portal', 'aggregator_list'}


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

    # LINE short links should be blocked before SEMI_AUTO candidate selection.
    lead = make_lead(url='https://lin.ee/abc123', title='LINE予約')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert 'lin.ee' in reason

    # Local listing/media portals should be blocked from source output.
    lead = make_lead(url='https://findglocal.com/JP/Fukuoka', title='掲載情報')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert 'findglocal' in reason or 'blocked_keyword' in reason

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


def test_government_domain_excluded_with_reason():
    lead = make_lead(url='https://www.city.example.lg.jp/page', title='自治体ページ')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert reason == 'domain:gov_association'


def test_global_media_domain_filtered_unless_local_pattern():
    lead = make_lead(url='https://forbes.com/some-article', title='World ranking')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert 'domain:global_media:forbes.com' == reason

    # Local business pattern should bypass global-media filter.
    local_like = make_lead(
        url='https://forbes.com/fukuoka-private-salon',
        title='福岡 個人サロン 完全予約',
        visible_text='個人カウンセリング サービス案内',
    )
    is_filtered, reason = get_filter_reason(local_like)
    assert not is_filtered
    assert reason == ''


def test_english_corporate_terms_excluded():
    lead = make_lead(url='https://example.com', title='Example Salon Inc.')
    is_filtered, reason = get_filter_reason(lead)
    assert is_filtered
    assert reason == 'corporate_franchise'


@pytest.mark.parametrize(
    "url,site_type",
    [
        ("https://small-salon.jimdofree.com/", "jimdofree"),
        ("https://owner-salon.jindo.com/", "jindo"),
        ("https://owner-studio.wixsite.com/fukuoka", "wixsite"),
        ("https://studio.site/fukuoka-studio", "studio.site"),
        ("https://owner-salon.amebaownd.com/", "amebaownd"),
        ("https://small-shop.stores.jp/", "stores"),
        ("https://owner-salon.base.shop/", "base.shop"),
        ("https://small-salon.goope.jp/", "goope"),
        ("https://crayon.e-shops.jp/salon/", "crayon"),
    ],
)
def test_simple_builder_platforms_get_positive_scoring_signal(url, site_type):
    lead = make_lead(
        url=url,
        title="福岡 個人サロン",
        visible_text="完全予約制、お問い合わせフォームあり",
        score=30,
        site_type=site_type,
    )
    boost, reasons = boost_score(lead)
    assert boost > 0
    assert not any(reason.startswith("penalize:platform_") for reason in reasons)
