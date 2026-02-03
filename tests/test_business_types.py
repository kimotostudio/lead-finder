from web_app import app as web_app


def test_business_types_includes_spiritual_items():
    expected = {
        "ヒーリング",
        "スピリチュアル",
        "エネルギーワーク",
        "レイキ",
        "チャネリング",
        "霊視",
        "オーラ鑑定",
        "波動調整",
        "浄化",
        "遠隔ヒーリング",
    }
    assert expected.issubset(set(web_app.BUSINESS_TYPES))


def test_query_generation_with_spiritual_items():
    """Test that 2-pass query generation works with spiritual items."""
    # Test Pass 1 (coarse) queries
    pass1_healing = web_app.build_pass1_queries("横浜市", "ヒーリング")
    pass1_remote = web_app.build_pass1_queries("横浜市", "遠隔ヒーリング")

    assert pass1_healing, "Pass 1 should generate queries for ヒーリング"
    assert pass1_remote, "Pass 1 should generate queries for 遠隔ヒーリング"
    assert any("横浜市 ヒーリング" in q for q in pass1_healing)
    assert any("横浜市 遠隔ヒーリング" in q for q in pass1_remote)

    # Test Pass 2 (expanded) queries for spiritual types
    pass2_healing = web_app.build_pass2_queries("横浜市", "ヒーリング")
    assert len(pass2_healing) > len(pass1_healing), "Spiritual types should have expanded Pass 2"


def test_business_type_categories_structure():
    """Verify categorized business types have correct structure."""
    categories = web_app.BUSINESS_TYPE_CATEGORIES
    assert len(categories) == 8, f"Expected 8 categories, got {len(categories)}"

    all_types = []
    for cat in categories:
        assert 'id' in cat
        assert 'name' in cat
        assert 'icon' in cat
        assert 'types' in cat
        assert len(cat['types']) > 0, f"Category {cat['id']} has no types"
        all_types.extend(cat['types'])

    # No duplicates across categories
    assert len(all_types) == len(set(all_types)), "Duplicate business types found across categories"

    # Flat list matches categories
    assert all_types == web_app.BUSINESS_TYPES


def test_expanded_spiritual_types():
    """Verify new spiritual/divination types are included."""
    new_spiritual = {'タロット占い', '占星術', '手相占い', '数秘術', '風水', 'パワーストーン'}
    assert new_spiritual.issubset(web_app.SPIRITUAL_BUSINESS_TYPES)
    # All new types should also be in the flat list
    assert new_spiritual.issubset(set(web_app.BUSINESS_TYPES))
