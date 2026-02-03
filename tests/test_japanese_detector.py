"""Tests for src/japanese_detector.py — Japanese content detection module."""
from src.japanese_detector import (
    has_japanese_characters,
    japanese_char_ratio,
    is_japanese_url,
    is_definitely_overseas_url,
    estimate_japanese_from_title,
    classify_url_japanese,
)


class TestHasJapaneseCharacters:
    def test_hiragana(self):
        assert has_japanese_characters('こんにちは')

    def test_katakana(self):
        assert has_japanese_characters('カウンセリング')

    def test_kanji(self):
        assert has_japanese_characters('東京都')

    def test_mixed(self):
        assert has_japanese_characters('Hello こんにちは World')

    def test_ascii_only(self):
        assert not has_japanese_characters('Hello World')

    def test_empty(self):
        assert not has_japanese_characters('')

    def test_none(self):
        assert not has_japanese_characters(None)

    def test_numbers_only(self):
        assert not has_japanese_characters('12345')


class TestJapaneseCharRatio:
    def test_full_japanese(self):
        ratio = japanese_char_ratio('こんにちは')
        assert ratio == 1.0

    def test_no_japanese(self):
        ratio = japanese_char_ratio('Hello')
        assert ratio == 0.0

    def test_mixed(self):
        # 3 JP chars + 2 ASCII = 60%
        ratio = japanese_char_ratio('ABこんに')
        assert 0.5 < ratio < 0.7

    def test_empty(self):
        assert japanese_char_ratio('') == 0.0


class TestIsJapaneseUrl:
    def test_jp_tld(self):
        assert is_japanese_url('https://example.jp/')

    def test_co_jp_tld(self):
        assert is_japanese_url('https://example.co.jp/')

    def test_or_jp_tld(self):
        assert is_japanese_url('https://example.or.jp/')

    def test_peraichi_platform(self):
        assert is_japanese_url('https://peraichi.com/landing/mypage')

    def test_jimdofree_platform(self):
        assert is_japanese_url('https://my-salon.jimdofree.com/')

    def test_ja_path(self):
        assert is_japanese_url('https://example.com/ja/about')

    def test_generic_com(self):
        assert not is_japanese_url('https://example.com/')

    def test_generic_net(self):
        assert not is_japanese_url('https://example.net/')

    def test_empty(self):
        assert not is_japanese_url('')


class TestIsDefinitelyOverseasUrl:
    def test_yelp(self):
        assert is_definitely_overseas_url('https://www.yelp.com/biz/some-business')

    def test_tripadvisor(self):
        assert is_definitely_overseas_url('https://tripadvisor.com/place/123')

    def test_booking(self):
        assert is_definitely_overseas_url('https://booking.com/hotel/jp-tokyo')

    def test_facebook(self):
        assert is_definitely_overseas_url('https://facebook.com/somepage')

    def test_foreign_tld_de(self):
        assert is_definitely_overseas_url('https://example.de/page')

    def test_foreign_tld_co_uk(self):
        assert is_definitely_overseas_url('https://example.co.uk/page')

    def test_foreign_tld_com_au(self):
        assert is_definitely_overseas_url('https://example.com.au/page')

    def test_foreign_tld_kr(self):
        assert is_definitely_overseas_url('https://example.kr/')

    def test_jp_domain_not_overseas(self):
        assert not is_definitely_overseas_url('https://example.jp/')

    def test_co_jp_not_overseas(self):
        assert not is_definitely_overseas_url('https://example.co.jp/')

    def test_generic_com_not_overseas(self):
        assert not is_definitely_overseas_url('https://example.com/')

    def test_peraichi_not_overseas(self):
        assert not is_definitely_overseas_url('https://peraichi.com/landing/page')

    def test_empty(self):
        assert not is_definitely_overseas_url('')


class TestEstimateJapaneseFromTitle:
    def test_full_japanese_title(self):
        assert estimate_japanese_from_title('横浜市のヒーリングサロン')

    def test_english_title(self):
        assert not estimate_japanese_from_title('Best Healing Salon in Town')

    def test_mixed_above_threshold(self):
        # Enough Japanese to pass 30%
        assert estimate_japanese_from_title('横浜ヒーリング salon')

    def test_mixed_below_threshold(self):
        # Very little Japanese
        assert not estimate_japanese_from_title('A very long English title with just one 字')

    def test_empty(self):
        assert not estimate_japanese_from_title('')

    def test_none(self):
        assert not estimate_japanese_from_title(None)


class TestClassifyUrlJapanese:
    def test_jp_url_is_japanese(self):
        assert classify_url_japanese('https://salon.jp/') == 'japanese'

    def test_overseas_domain_is_overseas(self):
        assert classify_url_japanese('https://yelp.com/biz/123') == 'overseas'

    def test_foreign_tld_is_overseas(self):
        assert classify_url_japanese('https://example.de/page') == 'overseas'

    def test_com_with_jp_title_is_japanese(self):
        assert classify_url_japanese('https://example.com/', 'ヒーリングサロン東京') == 'japanese'

    def test_com_without_title_is_uncertain(self):
        assert classify_url_japanese('https://example.com/') == 'uncertain'

    def test_com_with_english_title_is_uncertain(self):
        assert classify_url_japanese('https://example.com/', 'Some English Title') == 'uncertain'

    def test_jp_platform_is_japanese(self):
        assert classify_url_japanese('https://my-salon.jimdofree.com/') == 'japanese'

    def test_facebook_is_overseas(self):
        assert classify_url_japanese('https://facebook.com/mysalon') == 'overseas'

    def test_instagram_is_overseas(self):
        assert classify_url_japanese('https://instagram.com/mysalon') == 'overseas'
