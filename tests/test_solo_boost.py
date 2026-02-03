"""
Tests for solo/small business score boost functionality.
Ensures that solo practitioners with weak websites get boosted scores
to prevent them from being filtered out by min_score filters.
"""
import pytest
from src.scoring_rules import apply_solo_score_boost, apply_solo_boost_to_leads


class TestSoloScoreBoost:
    """Tests for the solo score boost function."""

    def test_peraichi_domain_gets_boost(self):
        """Peraichi sites should get a platform boost."""
        lead = {
            'url': 'https://peraichi.com/landing_pages/view/test',
            'shop_name': 'Test Salon',
            'solo_classification': 'unknown',
            'score': 30
        }
        boost, reasons = apply_solo_score_boost(lead)
        assert boost >= 40, f"Peraichi should get platform boost, got {boost}"
        assert any('peraichi' in r for r in reasons)

    def test_solo_classification_boost(self):
        """Leads with solo_classification='solo' should get +20."""
        lead = {
            'url': 'https://example.com',
            'solo_classification': 'solo',
            'score': 20
        }
        boost, reasons = apply_solo_score_boost(lead)
        assert 'solo_classified:+20' in reasons

    def test_small_classification_boost(self):
        """Leads with solo_classification='small' should get +10."""
        lead = {
            'url': 'https://example.com',
            'solo_classification': 'small',
            'score': 20
        }
        boost, reasons = apply_solo_score_boost(lead)
        assert 'small_classified:+10' in reasons

    def test_title_keywords_boost(self):
        """Title with solo keywords should get boost."""
        lead = {
            'url': 'https://example.com',
            'title': '完全予約制プライベートサロン',
            'solo_classification': 'unknown',
            'score': 30
        }
        boost, reasons = apply_solo_score_boost(lead)
        assert boost > 0, "Title keywords should provide boost"
        assert any('title_solo_kw' in r for r in reasons)

    def test_text_keywords_boost(self):
        """Text content with solo keywords should get boost."""
        lead = {
            'url': 'https://example.com',
            'visible_text': '一人で運営しています。自宅の一室でセラピーを行っています。',
            'solo_classification': 'unknown',
            'score': 30
        }
        boost, reasons = apply_solo_score_boost(lead)
        assert boost > 0, "Text keywords should provide boost"
        assert any('text_solo_kw' in r for r in reasons)

    def test_no_corp_info_bonus(self):
        """Sites without corporate info get a small bonus."""
        lead = {
            'url': 'https://example.com',
            'visible_text': 'このサロンは個人で運営しています。' * 20,  # Make it > 200 chars
            'solo_classification': 'unknown',
            'score': 30
        }
        boost, reasons = apply_solo_score_boost(lead)
        assert any('no_corp_info' in r for r in reasons)

    def test_corporate_site_no_bonus(self):
        """Sites with corporate info should not get the no_corp_info bonus."""
        lead = {
            'url': 'https://example.com',
            'visible_text': '株式会社テスト 会社概要 従業員数100名' + ('x' * 200),
            'solo_classification': 'corporate',
            'score': 50
        }
        boost, reasons = apply_solo_score_boost(lead)
        assert 'no_corp_info:+10' not in reasons

    def test_boost_capped_at_80(self):
        """Total boost should be capped at 80."""
        lead = {
            'url': 'https://peraichi.com/test',
            'title': '完全予約制 自宅サロン マンツーマン',
            'visible_text': '一人で運営 自宅の一室 個人事業主' * 10,
            'solo_classification': 'solo',
            'solo_score': 15,
            'score': 20
        }
        boost, _ = apply_solo_score_boost(lead)
        assert boost <= 80, f"Boost should be capped at 80, got {boost}"


class TestApplySoloBoostToLeads:
    """Tests for applying solo boost to multiple leads."""

    def test_boost_applied_to_leads(self):
        """Solo boost should be applied and tracked in leads."""
        leads = [
            {
                'url': 'https://peraichi.com/test',
                'solo_classification': 'solo',
                'score': 20
            },
            {
                'url': 'https://example.com',
                'solo_classification': 'unknown',
                'score': 40
            }
        ]

        result = apply_solo_boost_to_leads(leads)

        # First lead should have higher score now
        assert result[0]['score'] > 20, "Solo lead should have boosted score"
        assert result[0].get('solo_boost', 0) > 0, "solo_boost should be tracked"
        assert result[0].get('solo_boost_reasons'), "solo_boost_reasons should be present"

    def test_no_boost_for_unknown_without_indicators(self):
        """Unknown classification without indicators should get minimal boost."""
        leads = [
            {
                'url': 'https://generic-domain.com',
                'title': 'Generic Business',
                'visible_text': '',
                'solo_classification': 'unknown',
                'score': 40
            }
        ]

        result = apply_solo_boost_to_leads(leads)

        # Should get little to no boost
        assert result[0]['score'] <= 50, "Unknown without indicators should get minimal boost"


class TestSoloPlatformDetection:
    """Tests for platform-based solo detection."""

    @pytest.mark.parametrize("domain,expected_boost", [
        ("peraichi.com", 40),
        ("jimdofree.com", 35),
        ("wixsite.com", 30),
        ("crayonsite.net", 35),
        ("goope.jp", 30),
    ])
    def test_platform_domains(self, domain, expected_boost):
        """Various solo-friendly platforms should get appropriate boosts."""
        lead = {
            'url': f'https://{domain}/test',
            'solo_classification': 'unknown',
            'score': 30
        }
        boost, reasons = apply_solo_score_boost(lead)
        platform_boost = [r for r in reasons if 'platform_' in r]
        assert platform_boost, f"Should detect platform {domain}"
