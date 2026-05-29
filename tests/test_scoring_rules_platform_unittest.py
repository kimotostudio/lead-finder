import unittest

from src.scoring_rules import boost_score


class ScoringRulesPlatformTests(unittest.TestCase):
    def test_valid_platform_never_gets_thin_platform_penalty(self) -> None:
        cases = [
            ("https://peraichi.com/landing_pages/view/sample", "peraichi"),
            ("https://owner-salon.jimdofree.com/", "jimdofree"),
            ("https://owner-studio.wixsite.com/fukuoka", "wixsite"),
            ("https://studio.site/fukuoka-studio", "studio.site"),
            ("https://owner-salon.amebaownd.com/", "amebaownd"),
            ("https://small-shop.stores.jp/", "stores"),
            ("https://owner-salon.base.shop/", "base.shop"),
            ("https://small-salon.goope.jp/", "goope"),
            ("https://crayon.e-shops.jp/salon/", "crayon"),
        ]
        for url, site_type in cases:
            with self.subTest(site_type=site_type):
                lead = {
                    "url": url,
                    "shop_name": "サンプルページ",
                    "reasons": "",
                    "visible_text": "サンプル紹介ページ",
                    "site_type": site_type,
                }
                _boost, reasons = boost_score(lead)
                self.assertFalse(any(r.startswith("penalize:platform_") for r in reasons))


if __name__ == "__main__":
    unittest.main()
