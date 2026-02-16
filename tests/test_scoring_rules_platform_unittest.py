import unittest

from src.scoring_rules import boost_score


class ScoringRulesPlatformTests(unittest.TestCase):
    def test_valid_platform_never_gets_thin_platform_penalty(self) -> None:
        lead = {
            "url": "https://peraichi.com/landing_pages/view/sample",
            "shop_name": "サンプルページ",
            "reasons": "",
            "visible_text": "サンプル紹介ページ",
            "site_type": "peraichi",
        }
        _boost, reasons = boost_score(lead)
        self.assertFalse(any(r.startswith("penalize:platform_") for r in reasons))


if __name__ == "__main__":
    unittest.main()
