"""
Tests for URL filtering and query generation improvements in app.py
"""
import pytest
from web_app.app import is_blocked_url, build_exclude_clause, build_pass1_queries, build_pass2_queries, build_pass3_queries, prioritize_urls, is_foreign_url, prefilter_urls


class TestIsBlockedUrl:
    """Tests for the is_blocked_url function."""

    def test_ameblo_blocked(self):
        """Ameblo URLs should be blocked."""
        assert is_blocked_url('https://ameblo.jp/test')
        assert is_blocked_url('https://www.ameblo.jp/test')
        assert is_blocked_url('https://s.ameblo.jp/test')

    def test_note_blocked(self):
        """Note.com URLs should be blocked."""
        assert is_blocked_url('https://note.com/user')
        assert is_blocked_url('https://note.mu/user')

    def test_hotpepper_blocked(self):
        """Hotpepper URLs should be blocked."""
        assert is_blocked_url('https://beauty.hotpepper.jp/salon')
        assert is_blocked_url('https://hotpepper.jp/str')

    def test_fc2_blocked(self):
        """FC2 URLs should be blocked."""
        assert is_blocked_url('https://fc2.com/test')
        assert is_blocked_url('https://blog.fc2.com/test')
        assert is_blocked_url('https://fc2blog.net/test')

    def test_lit_link_blocked(self):
        """lit.link URLs should be blocked."""
        assert is_blocked_url('https://lit.link/user')
        assert is_blocked_url('https://linktr.ee/user')

    def test_google_maps_blocked(self):
        """Google Maps URLs should be blocked."""
        assert is_blocked_url('https://google.com/maps/place/test')
        assert is_blocked_url('https://www.google.com/maps?q=test')
        assert is_blocked_url('https://maps.google.com/test')
        assert is_blocked_url('https://goo.gl/maps/test')

    def test_independent_domain_allowed(self):
        """Independent domains should not be blocked."""
        assert not is_blocked_url('https://example.com')
        assert not is_blocked_url('https://my-salon.jp')
        assert not is_blocked_url('https://healing-room.com')

    def test_peraichi_allowed(self):
        """Peraichi URLs should be allowed (solo-friendly platform)."""
        assert not is_blocked_url('https://peraichi.com/landing_pages/view/test')

    def test_directory_patterns_blocked(self):
        """URLs with directory patterns should be blocked."""
        assert is_blocked_url('https://example.com/ranking')
        assert is_blocked_url('https://example.com/口コミ')
        assert is_blocked_url('https://example.com/おすすめ')

    def test_empty_url_blocked(self):
        """Empty URLs should be blocked."""
        assert is_blocked_url('')
        assert is_blocked_url(None)


class TestBuildExcludeClause:
    """Tests for the build_exclude_clause function."""

    def test_returns_string(self):
        """Should return a string."""
        result = build_exclude_clause()
        assert isinstance(result, str)

    def test_contains_ameblo(self):
        """Should contain ameblo exclusion."""
        result = build_exclude_clause()
        assert '-site:ameblo.jp' in result

    def test_contains_note(self):
        """Should contain note.com exclusion."""
        result = build_exclude_clause()
        assert '-site:note.com' in result

    def test_contains_hotpepper(self):
        """Should contain hotpepper exclusion."""
        result = build_exclude_clause()
        assert '-site:hotpepper.jp' in result


class TestTwoPassQueryGeneration:
    """Tests for the 2-pass query generation functions."""

    def test_pass1_returns_list(self):
        """Pass 1 should return a list of queries."""
        queries = build_pass1_queries('新宿区', 'カウンセリング')
        assert isinstance(queries, list)
        assert len(queries) > 0

    def test_pass1_is_coarse(self):
        """Pass 1 should generate exactly 15 queries per pair (including JP-biased)."""
        queries = build_pass1_queries('新宿区', 'カウンセリング')
        assert len(queries) == 15, "Pass 1 should have exactly 15 queries"

    def test_pass1_contains_city_and_btype(self):
        """Pass 1 queries should contain city and business type."""
        queries = build_pass1_queries('渋谷区', 'セラピー')
        assert all('渋谷区' in q for q in queries)
        assert all('セラピー' in q for q in queries)

    def test_pass1_has_exclusions(self):
        """Pass 1 queries should have -site: exclusions."""
        queries = build_pass1_queries('目黒区', 'ヒーリング')
        # All Pass 1 queries should have exclusion clauses
        assert all('-site:' in q for q in queries)

    def test_pass1_includes_pricing_and_booking(self):
        """Pass 1 queries should include 料金 and 予約 signals."""
        queries = build_pass1_queries('池袋', 'コーチング')
        query_text = ' '.join(queries)
        assert '料金' in query_text, "Should include pricing signal"
        assert '予約' in query_text, "Should include booking signal"

    def test_pass2_returns_list(self):
        """Pass 2 should return a list of queries."""
        queries = build_pass2_queries('新宿区', 'カウンセリング')
        assert isinstance(queries, list)
        assert len(queries) > 0

    def test_pass2_is_expanded(self):
        """Pass 2 should have more queries than Pass 1."""
        pass1 = build_pass1_queries('新宿区', 'カウンセリング')
        pass2 = build_pass2_queries('新宿区', 'カウンセリング')
        assert len(pass2) > len(pass1), "Pass 2 should have more queries"

    def test_pass2_contains_solo_signals(self):
        """Pass 2 should include solo/small business signals."""
        queries = build_pass2_queries('品川区', '整体')
        query_text = ' '.join(queries)
        # Should have solo signals
        assert '個人' in query_text or '自宅' in query_text, "Should have solo signals"

    def test_pass2_contains_platforms(self):
        """Pass 2 should include platform-specific queries."""
        queries = build_pass2_queries('代々木', 'ヨガ')
        query_text = ' '.join(queries)
        # Should target specific platforms
        assert 'site:peraichi.com' in query_text or 'site:jimdofree.com' in query_text

    def test_pass2_no_ameblo_target(self):
        """Pass 2 should not target ameblo positively."""
        queries = build_pass2_queries('新宿区', 'カウンセリング')
        for q in queries:
            if 'site:ameblo' in q.lower():
                assert '-site:ameblo' in q.lower(), f"Ameblo should only be exclusion: {q}"

    def test_spiritual_types_get_extra_pass2(self):
        """Spiritual business types should get extra Pass 2 queries."""
        queries_spiritual = build_pass2_queries('代々木', 'ヒーリング')
        queries_normal = build_pass2_queries('代々木', 'カウンセリング')
        # Spiritual types should have additional queries
        assert len(queries_spiritual) > len(queries_normal), "Spiritual should have extra queries"

    def test_no_duplicate_queries(self):
        """Pass 1 and Pass 2 should not have duplicates within themselves."""
        pass1 = build_pass1_queries('新宿区', 'カウンセリング')
        pass2 = build_pass2_queries('新宿区', 'カウンセリング')
        assert len(pass1) == len(set(pass1)), "Pass 1 should not have duplicates"
        assert len(pass2) == len(set(pass2)), "Pass 2 should not have duplicates"


class TestPrioritizeUrls:
    """Tests for URL prioritization function."""

    def test_returns_list(self):
        """Should return a list."""
        urls = ['https://example.com', 'https://test.jp']
        result = prioritize_urls(urls)
        assert isinstance(result, list)
        assert len(result) == len(urls)

    def test_own_domain_before_builder(self):
        """Own-domain URLs should come before builder platform URLs."""
        urls = [
            'https://peraichi.com/user/salon',
            'https://my-salon.jp/',
        ]
        result = prioritize_urls(urls)
        assert result[0] == 'https://my-salon.jp/', "Own domain should be first"

    def test_root_path_before_deep_path(self):
        """Root/short paths should come before deep paths."""
        urls = [
            'https://example.com/blog/2024/01/post',
            'https://example.com/',
        ]
        result = prioritize_urls(urls)
        assert result[0] == 'https://example.com/', "Root path should be first"

    def test_business_paths_prioritized(self):
        """Business-related paths should be prioritized."""
        urls = [
            'https://example.com/random-page',
            'https://example.com/menu',
        ]
        result = prioritize_urls(urls)
        assert result[0] == 'https://example.com/menu', "Business path should be first"

    def test_handles_empty_list(self):
        """Should handle empty list."""
        result = prioritize_urls([])
        assert result == []

    def test_handles_invalid_urls(self):
        """Should handle invalid URLs gracefully."""
        urls = ['not-a-url', 'https://valid.com']
        result = prioritize_urls(urls)
        assert len(result) == 2

    def test_jp_domain_before_foreign(self):
        """JP domain URLs should be prioritized before non-JP domains."""
        urls = [
            'https://example.com/',
            'https://my-salon.jp/',
        ]
        result = prioritize_urls(urls)
        assert result[0] == 'https://my-salon.jp/', ".jp domain should be first"


class TestIsForeignUrl:
    """Tests for the is_foreign_url function."""

    def test_jp_domain_not_foreign(self):
        """Japanese .jp domains should not be flagged as foreign."""
        assert not is_foreign_url('https://example.jp')
        assert not is_foreign_url('https://salon.co.jp')
        assert not is_foreign_url('https://clinic.or.jp')
        assert not is_foreign_url('https://school.ac.jp')
        assert not is_foreign_url('https://provider.ne.jp')

    def test_foreign_tld_detected(self):
        """Foreign TLDs should be detected and blocked."""
        assert is_foreign_url('https://example.de')
        assert is_foreign_url('https://salon.fr')
        assert is_foreign_url('https://clinic.co.uk')
        assert is_foreign_url('https://therapy.com.au')
        assert is_foreign_url('https://healing.in')

    def test_jp_platforms_allowed(self):
        """Known JP platform domains should not be flagged as foreign."""
        assert not is_foreign_url('https://peraichi.com/landing/test')
        assert not is_foreign_url('https://user.jimdofree.com')
        assert not is_foreign_url('https://user.wixsite.com/salon')
        assert not is_foreign_url('https://wordpress.com/site')
        assert not is_foreign_url('https://studio.site/test')

    def test_generic_com_allowed(self):
        """.com domains not in foreign TLDs should be allowed."""
        assert not is_foreign_url('https://my-business.com')
        assert not is_foreign_url('https://healing-salon.com')

    def test_empty_url(self):
        """Empty/invalid URLs should not be flagged as foreign."""
        assert not is_foreign_url('')
        assert not is_foreign_url('not-a-url')


class TestPrefilterForeignUrls:
    """Tests for foreign URL removal in prefilter_urls."""

    def test_prefilter_removes_foreign_tlds(self):
        """prefilter_urls should remove URLs with foreign TLDs."""
        urls = [
            'https://my-salon.jp/',
            'https://example.de/page',
            'https://healing.co.uk/about',
            'https://therapy-tokyo.com/',
            'https://user.peraichi.com/landing',
        ]
        result = prefilter_urls(urls, max_per_domain=5)
        domains = [url.split('/')[2].replace('www.', '') for url in result]
        # Foreign TLDs should be removed
        assert not any('.de' in d for d in domains), "German TLD should be removed"
        assert not any('.co.uk' in d for d in domains), "UK TLD should be removed"
        # JP and allowed platforms should remain
        assert any('.jp' in d for d in domains), ".jp domain should remain"
        assert any('peraichi.com' in d for d in domains), "JP platform should remain"

    def test_prefilter_keeps_jp_domains(self):
        """prefilter_urls should keep all .jp domains."""
        urls = [
            'https://salon.jp/',
            'https://clinic.co.jp/',
            'https://school.or.jp/',
        ]
        result = prefilter_urls(urls, max_per_domain=10)
        assert len(result) == 3, "All .jp domains should be kept"


class TestJpBiasedQueries:
    """Tests for JP-biased query generation."""

    def test_pass1_includes_site_jp(self):
        """Pass 1 should include a site:.jp query."""
        queries = build_pass1_queries('新宿区', 'カウンセリング')
        assert any('site:.jp' in q for q in queries), "Pass 1 should have site:.jp query"

    def test_pass1_includes_official(self):
        """Pass 1 should include 公式 (official) query."""
        queries = build_pass1_queries('渋谷区', 'セラピー')
        assert any('公式' in q for q in queries), "Pass 1 should have 公式 query"

    def test_pass2_includes_jp_signals(self):
        """Pass 2 should include JP-biased signals."""
        queries = build_pass2_queries('品川区', '整体')
        query_text = ' '.join(queries)
        assert '営業時間' in query_text or 'site:.jp' in query_text, \
            "Pass 2 should have JP-biased signals"


class TestPass3VariationQueries:
    """Tests for Pass 3 variation query generation."""

    def test_pass3_returns_list(self):
        queries = build_pass3_queries('横浜市', 'カウンセリング')
        assert isinstance(queries, list)
        assert len(queries) > 0

    def test_pass3_has_comparison_patterns(self):
        queries = build_pass3_queries('横浜市', 'ヒーリング')
        query_text = ' '.join(queries)
        assert '比較' in query_text, "Pass 3 should have comparison query"
        assert 'ランキング' in query_text, "Pass 3 should have ranking query"

    def test_pass3_city_variation_adds_station(self):
        """Pass 3 should add 駅前 variation."""
        queries = build_pass3_queries('横浜市', '整体')
        assert any('駅前' in q for q in queries), "Pass 3 should have station variation"

    def test_pass3_city_ending_with_shi_no_double_suffix(self):
        """If city already ends with 市, don't add 市 again."""
        queries = build_pass3_queries('横浜市', '整体')
        assert not any('横浜市市' in q for q in queries), "Should not double 市 suffix"

    def test_pass3_has_exclusion_clauses(self):
        """Pass 3 queries should have exclusion clauses."""
        queries = build_pass3_queries('新宿区', 'カウンセリング')
        assert any('-site:' in q for q in queries), "Pass 3 queries should have exclusion clauses"
