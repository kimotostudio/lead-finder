from src.scoring_rules import compute_weakness_for_lead, compute_weakness


def make_lead(**kwargs):
    return {
        'html': kwargs.get('html', ''),
        'visible_text': kwargs.get('visible_text', ''),
        'shop_name': kwargs.get('shop_name', ''),
        'title': kwargs.get('title', ''),
        'reasons': kwargs.get('reasons', ''),
        'email': kwargs.get('email', ''),
        'business_hours': kwargs.get('business_hours', ''),
    }


def test_old_site_detected():
    lead = make_lead(html='... \u00a9 2014 Some Company ...')
    w_score, reasons = compute_weakness_for_lead(lead)
    assert w_score >= 15
    # Japanese reason format: 更新{n}年以上前 or 更新{n}年前
    assert any('更新' in r for r in reasons)


def test_missing_reservation_and_form_with_mailto():
    lead = make_lead(visible_text='Service info only', email='info@example.com')
    w_score, reasons = compute_weakness_for_lead(lead)
    # Japanese reason format: 予約システムなし instead of no_reservation
    assert any('予約' in r for r in reasons)
    # Score should be elevated due to missing features
    assert w_score >= 20


def test_viewport_present_no_viewport_penalty():
    lead = make_lead(html='<meta name="viewport" content="width=device-width">')
    w_score, reasons = compute_weakness_for_lead(lead)
    assert 'no_viewport' not in reasons


def test_compute_weakness_batch_and_types():
    leads = [
        make_lead(html='\u00a9 2014', visible_text=''),
        make_lead(html='', visible_text='\u4e88\u7d04\u30d5\u30a9\u30fc\u30e0\u304c\u3042\u308a'),
    ]
    updated = compute_weakness(leads)
    for lead in updated:
        assert isinstance(lead.get('weakness_score', 0), int)
        assert lead.get('weakness_score', 0) >= 0
        assert isinstance(lead.get('weakness_reasons', []), list)
