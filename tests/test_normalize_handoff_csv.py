from tools.normalize_handoff_csv import normalize_row


def test_confirmed_contact_page_form_canonicalizes_form_like_path() -> None:
    row = {
        "domain": "harajimusyo.net",
        "url": "https://harajimusyo.net/",
        "display_name": "原司法書士事務所",
        "title": "原司法書士事務所 | 福岡市中央区大名の司法書士事務所です。",
        "contact_url": "https://harajimusyo.net/otoiawase/",
        "has_form": "True",
        "form_url": "https://harajimusyo.net/wordpress/otoiawase/",
        "address": "営業時間 9:30～17:00 ホーム Home",
        "area_guess": "中央区",
        "reason": (
            "unknown | wordpress; no_booking | contact_fetch_status=200 | "
            "contact_page_title=お問い合わせ | 原司法書士事務所 | "
            "contact_page_address=〒810-0041 福岡市中央区大名1丁目9番27号第一西部ビル TEL: 092-738-3038 | "
            "contact_page_has_form=True"
        ),
    }

    normalized = normalize_row(row, source_csv="fixture.csv", source_row=1)

    assert normalized["contact_url"] == "https://harajimusyo.net/otoiawase/"
    assert normalized["form_url"] == "https://harajimusyo.net/otoiawase/"
    assert normalized["original__form_url"] == "https://harajimusyo.net/otoiawase/"
    assert normalized["original__raw_form_url"] == "https://harajimusyo.net/wordpress/otoiawase/"
    assert normalized["original__has_form"] == "True"
    assert normalized["original__form_evidence_kind"] == "confirmed_same_site_contact_page_form"
    assert normalized["original__contact_page_has_form"] == "True"
    assert "multiple_same_site_contact_paths" in normalized["original__contact_path_ambiguity"]
    assert "form_url=https://harajimusyo.net/wordpress/otoiawase/" in normalized["original__contact_path_ambiguity"]
    assert "福岡市中央区大名" in normalized["original__address"]
    assert normalized["original__raw_address"] == "営業時間 9:30～17:00 ホーム Home"


def test_contact_page_ward_address_fills_gate_consumed_address_without_confirming_missing_form() -> None:
    row = {
        "domain": "www.satoc-office.com",
        "url": "https://www.satoc-office.com/",
        "display_name": "白金司法書士事務所",
        "title": "白金司法書士事務所｜法的問題に寄り添う司法書士のパートナー",
        "contact_url": "https://www.satoc-office.com/contact.php",
        "has_form": "True",
        "form_url": "https://www.satoc-office.com/contact.php",
        "address": "お問い合わせ トップ 事業所概要 業務内容 お問い合わ",
        "area_guess": "中央区",
        "reason": (
            "solo | no_booking | contact_fetch_status=200 | "
            "contact_page_title=お問い合わせ｜白金司法書士事務所 | "
            "contact_page_address=〒810-0012 福岡市中央区白金1丁目5-21 TEL: 092-401-1654 | "
            "contact_page_has_form=False"
        ),
    }

    normalized = normalize_row(row, source_csv="fixture.csv", source_row=3)

    assert normalized["domain"] == "satoc-office.com"
    assert "福岡市中央区白金" in normalized["original__address"]
    assert normalized["address_evidence_source"] == "contact_page_address"
    assert normalized["original__raw_address"] == "お問い合わせ トップ 事業所概要 業務内容 お問い合わ"
    assert normalized["original__single_location_evidence"] == "fukuoka_ward_address:中央区"
    assert normalized["original__has_form"] == "False"
    assert normalized["original__form_evidence_kind"] == "confirmed_same_site_contact_page_no_form"


def test_corporate_identity_signal_is_carried_not_cleared() -> None:
    row = {
        "domain": "ito-kaik.com",
        "url": "https://ito-kaik.com/",
        "display_name": "伊藤会計事務所",
        "title": "福岡の税理士事務所 | 伊藤会計事務所",
        "contact_url": "https://ito-kaik.com/contact",
        "has_form": "True",
        "form_url": "https://ito-kaik.com/contact",
        "address": "福岡市中央区薬院3-16-26西鉄薬院ビル5階",
        "area_guess": "中央区",
        "reason": (
            "corporate | wordpress; no_booking | contact_fetch_status=200 | "
            "contact_page_address=〒810-0022 福岡市中央区薬院3-16-26西鉄薬院ビル5階 | "
            "contact_page_has_form=True"
        ),
    }

    normalized = normalize_row(row, source_csv="fixture.csv", source_row=2)

    assert normalized["identity_signal"] == "corporate"
    assert normalized["original__identity_signal"] == "corporate"
    assert "corporate" in normalized["notes"]
    assert "福岡市中央区薬院" in normalized["original__address"]
