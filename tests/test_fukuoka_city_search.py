import json
import tempfile
import unittest
from pathlib import Path

from tools.run_fukuoka_city_search import (
    apply_query_filters,
    build_fukuoka_queries,
    load_search_config,
    sort_urls_deterministically,
)


class FukuokaCitySearchTests(unittest.TestCase):
    def test_load_config_and_build_queries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "cfg.json"
            cfg = {
                "city": "福岡市",
                "queries": [
                    "福岡市 中央区 整体 自宅サロン お問い合わせフォーム",
                    "福岡市 天神 ネイル 完全予約制 ネット予約",
                    "福岡市 中央区 整体 自宅サロン お問い合わせフォーム"
                ],
                "negatives": ["株式会社", "採用"],
                "required_markers": {
                    "regional": ["中央区", "天神"],
                    "business": ["整体", "ネイル"],
                    "solo": ["自宅サロン", "完全予約制"],
                },
            }
            cfg_path.write_text(json.dumps(cfg, ensure_ascii=False), encoding="utf-8")
            loaded = load_search_config(cfg_path)
            queries = build_fukuoka_queries(loaded, max_queries=10)
            self.assertEqual(len(queries), 2)
            self.assertEqual(len(queries), len(set(queries)))
            self.assertIn("福岡市 中央区 整体 自宅サロン お問い合わせフォーム", queries)
            self.assertIn("福岡市 天神 ネイル 完全予約制 ネット予約", queries)

    def test_sort_urls_is_deterministic(self) -> None:
        urls = [
            "https://b.example.com/path?x=1",
            "https://a.example.com/z",
            "https://www.a.example.com/z?utm_source=x",
            "https://b.example.com/path",
        ]
        s1 = sort_urls_deterministically(urls)
        s2 = sort_urls_deterministically(list(reversed(urls)))
        self.assertEqual(s1, s2)
        self.assertEqual(s1[0], "https://a.example.com/z")

    def test_apply_query_filters_negatives_and_required_markers(self) -> None:
        queries = [
            "福岡市 中央区 整体 自宅サロン お問い合わせフォーム",
            "福岡市 中央区 整体 株式会社 お問い合わせフォーム",
            "福岡市 中央区 自宅サロン お問い合わせフォーム",
        ]
        filtered = apply_query_filters(
            queries,
            negatives=["株式会社"],
            required_markers={"regional": ["中央区"], "business": ["整体"], "solo": ["自宅サロン"]},
        )
        self.assertEqual(filtered, ["福岡市 中央区 整体 自宅サロン お問い合わせフォーム"])


if __name__ == "__main__":
    unittest.main()
