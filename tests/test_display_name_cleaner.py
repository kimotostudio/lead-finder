from tools.display_name_cleaner import clean_display_name, clean_row_names


def _row(title: str, domain: str) -> dict[str, str]:
    return {
        "title": title,
        "website": f"https://{domain}/",
        "domain": domain,
        "display_name": title,
        "name_source": "title",
    }


def test_generic_title_falls_back_to_domain_name() -> None:
    result = clean_display_name(_row("トップページ", "healing-casablanca.com"))

    assert result.display_name == "Healing Casablanca"
    assert result.name_source == "domain"
    assert result.name_confidence == "medium"
    assert "domain_fallback" in result.name_warning


def test_promotional_quoted_title_extracts_business_name() -> None:
    result = clean_display_name(_row("「匠 天神駅前整骨院」医師・専門家が絶賛", "fukuokatenjin.seikotsu-takumi.com"))

    assert result.display_name == "匠 天神駅前整骨院"
    assert result.name_source == "title_cleaned"
    assert result.name_confidence == "high"


def test_promotional_title_falls_back_to_domain_name() -> None:
    result = clean_display_name(_row("その他全教室の無料体験レッスン実施中", "soul-meeting.com"))

    assert result.display_name == "Soul Meeting"
    assert result.name_source == "domain"
    assert "domain_fallback" in result.name_warning


def test_category_list_title_falls_back_to_domain_name() -> None:
    result = clean_display_name(_row("ヨガ、ホットヨガ、ピラティス、エアリアルヨガ", "yoga-mii.com"))

    assert result.display_name == "Yoga Mii"
    assert result.name_source == "domain"
    assert "domain_fallback" in result.name_warning


def test_generic_english_title_falls_back_to_domain_name() -> None:
    result = clean_display_name(_row("Music room", "piatomofukuoka.com"))

    assert result.display_name == "Piatomo Fukuoka"
    assert result.name_source == "domain"
    assert "domain_fallback" in result.name_warning


def test_pipe_title_prefers_business_segment() -> None:
    result = clean_display_name(_row("Healing Casablanca | 福岡のヒーリングサロン", "healing-casablanca.com"))

    assert result.display_name == "Healing Casablanca"
    assert result.name_source == "title_cleaned"
    assert result.name_confidence == "high"


def test_dash_title_prefers_business_segment() -> None:
    result = clean_display_name(_row("Soul Meeting - 福岡の音楽教室", "soul-meeting.com"))

    assert result.display_name == "Soul Meeting"
    assert result.name_source == "title_cleaned"
    assert result.name_confidence == "high"


def test_fullwidth_pipe_title_prefers_business_segment() -> None:
    result = clean_display_name(_row("Studio Haku｜ヨガ・ピラティススタジオ", "studio-haku.com"))

    assert result.display_name == "Studio Haku"
    assert result.name_source == "title_cleaned"
    assert result.name_confidence == "high"


def test_clean_row_names_updates_downstream_aliases() -> None:
    row = _row("トップページ", "healing-casablanca.com")
    cleaned, result = clean_row_names(row)

    assert result.display_name == "Healing Casablanca"
    for field in ["display_name", "business_name", "company_name", "salon_name", "brand_name", "店名"]:
        assert cleaned[field] == "Healing Casablanca"
    assert cleaned["original_display_name"] == "トップページ"
    assert cleaned["original_title"] == "トップページ"
