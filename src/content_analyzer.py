"""
Advanced content analyzer for extracting business information and detecting aggregators.
"""
import re
import logging
from typing import Dict, Optional, List
from bs4 import BeautifulSoup
from config.keywords import (
    AGGREGATOR_KEYWORDS, OWNER_KEYWORDS, BUSINESS_NAME_PATTERNS,
    CITY_KEYWORDS
)

logger = logging.getLogger(__name__)

# Pre-compiled regexes for Japanese character detection
_RE_HIRAGANA = re.compile(r'[\u3040-\u309F]')
_RE_KATAKANA = re.compile(r'[\u30A0-\u30FF]')
_RE_KANJI = re.compile(r'[\u4E00-\u9FFF]')

# Patterns for extracting Japanese business names
_JP_BUSINESS_NAME_PATTERNS = [
    # Business name + suffix (e.g. 〇〇サロン)
    re.compile(
        r'([ぁ-んァ-ヶー一-龠々\s]{2,15}'
        r'(?:サロン|カウンセリング|整体|ヨガ|エステ|セラピー|占い|コーチング|'
        r'クリニック|院|事務所|スタジオ|ルーム|スクール|教室|治療院|鍼灸|'
        r'リラクゼーション|ヒーリング|ピラティス|ジム|アカデミー))'
    ),
    # General Japanese text 2-20 chars
    re.compile(r'([ぁ-んァ-ヶー一-龠々]{2,20})'),
]


def has_japanese_content(text: str) -> bool:
    """Check if text contains Japanese characters (hiragana, katakana, or kanji)."""
    if not text:
        return False
    return bool(_RE_HIRAGANA.search(text) or _RE_KATAKANA.search(text) or _RE_KANJI.search(text))


def extract_japanese_name(text: str, max_len: int = 50) -> str:
    """
    Extract the first Japanese business name from text.

    Tries business-suffix patterns first, then falls back to any Japanese phrase.

    Returns:
        Japanese name string, or empty string if none found.
    """
    if not text:
        return ''
    for pattern in _JP_BUSINESS_NAME_PATTERNS:
        match = pattern.search(text)
        if match:
            name = match.group(1).strip()
            if len(name) >= 2:
                return name[:max_len]
    return ''


class ContentAnalyzer:
    """Analyzes webpage content to extract business details and detect aggregators."""

    def is_aggregator_site(self, title: str, html: str, visible_text: str) -> bool:
        """
        Detect if the site is an aggregator/review/listing site.

        Args:
            title: Page title
            html: HTML content
            visible_text: Visible text content

        Returns:
            True if site is aggregator, False if individual business
        """
        # Strong aggregator keywords in title
        strong_keywords = ['おすすめ', 'ランキング', '比較', '一覧', 'まとめ', 'best', 'top']
        title_lower = title.lower()

        for keyword in strong_keywords:
            if keyword in title:
                logger.debug(f"Aggregator detected in title: {keyword}")
                return True

        # Check for list patterns (e.g., "おすすめ20選")
        list_pattern = r'[0-9]{1,3}選'
        if re.search(list_pattern, title):
            logger.debug(f"List pattern detected in title: {title}")
            return True

        # Check for multiple business listings in content (threshold raised)
        business_count = sum(1 for pattern in BUSINESS_NAME_PATTERNS if visible_text.count(pattern) > 5)
        if business_count >= 3:
            logger.debug(f"Multiple businesses detected (aggregator)")
            return True

        # Check for portal/listing language
        portal_indicators = ['掲載店舗', '登録数', '一括予約', '比較サイト', 'portal']
        portal_count = sum(1 for ind in portal_indicators if ind in visible_text)
        if portal_count >= 2:
            logger.debug(f"Portal language detected (aggregator)")
            return True

        # Check for high review/rating density (raised threshold)
        review_indicators = ['評価', '評判', '口コミ', '★★', '☆☆', 'レビュー']
        review_count = sum(1 for ind in review_indicators if visible_text.count(ind) > 8)
        if review_count >= 2:
            logger.debug(f"High review content detected (aggregator)")
            return True

        return False

    def extract_shop_name(self, url: str, title: str, h1: str, soup: BeautifulSoup, visible_text: str) -> str:
        """
        Extract the actual shop/business name from page.

        Priority:
        1. H1 with business pattern
        2. Title cleaned with business pattern
        3. Meta og:site_name
        4. Header logo text
        5. Domain name fallback

        Args:
            url: Page URL
            title: Page title
            h1: H1 heading
            soup: BeautifulSoup object
            visible_text: Visible text

        Returns:
            Shop name
        """
        # Clean title for extraction
        clean_title = title
        remove_suffixes = [
            '｜', '|', ' - ', ' – ', '【', '】',
            'のご案内', 'のホームページ', 'のウェブサイト', '公式サイト',
            'Official Site', 'Home', 'ホーム',
        ]
        for suffix in remove_suffixes:
            if suffix in clean_title:
                clean_title = clean_title.split(suffix)[0]
        clean_title = clean_title.strip()

        # H1 with business pattern
        if h1:
            if len(h1) < 50 and any(pattern in h1 for pattern in BUSINESS_NAME_PATTERNS):
                return self._clean_shop_name(h1)

        # Title with business pattern
        if len(clean_title) < 50 and any(pattern in clean_title for pattern in BUSINESS_NAME_PATTERNS):
            return self._clean_shop_name(clean_title)

        # Meta og:site_name
        og_site_name = soup.find('meta', property='og:site_name')
        if og_site_name and og_site_name.get('content'):
            site_name = og_site_name['content'].strip()
            if len(site_name) < 50:
                return self._clean_shop_name(site_name)

        # Header logo text
        header = soup.find('header')
        if header:
            logo = header.find(['h1', 'div'], class_=re.compile(r'logo|brand|site-name', re.I))
            if logo:
                logo_text = logo.get_text(strip=True)
                if logo_text and len(logo_text) < 50:
                    return self._clean_shop_name(logo_text)

        # Domain name fallback
        from urllib.parse import urlparse
        domain = urlparse(url).netloc
        domain = domain.replace('www.', '').replace('www2.', '')
        domain_name = domain.split('.')[0]
        if domain_name and len(domain_name) > 2:
            return domain_name

        # Last resort: use cleaned title or Unknown
        return self._clean_shop_name(clean_title) if clean_title else "Unknown"

    def _clean_shop_name(self, name: str) -> str:
        """Clean and normalize shop name."""
        # Remove extra whitespace
        name = ' '.join(name.split())
        # Remove common prefixes/suffixes
        name = name.replace('公式サイト', '').replace('Official', '').replace('ホーム', '')
        name = name.strip(' -|【】')
        return name[:100]  # Limit length

    def extract_owner_name(self, html: str, soup: BeautifulSoup, visible_text: str) -> str:
        """
        Extract owner/proprietor name from content.

        Args:
            html: HTML content
            soup: BeautifulSoup object
            visible_text: Visible text

        Returns:
            Owner name if found, empty string otherwise
        """
        # Look for owner section
        for keyword in OWNER_KEYWORDS:
            # Find sections mentioning owner
            sections = soup.find_all(['div', 'section', 'p'], string=re.compile(keyword))
            for section in sections:
                text = section.get_text(strip=True)
                # Extract potential name (katakana, hiragana, kanji)
                name_pattern = r'([ぁ-んァ-ヶー一-龯]{2,10})\s*([ぁ-んァ-ヶー一-龯]{1,10})?'
                matches = re.findall(name_pattern, text)
                if matches:
                    # Get first reasonable match
                    full_name = ' '.join(filter(None, matches[0]))
                    if full_name and len(full_name) >= 2:
                        return full_name[:30]

        # Check meta author
        author_meta = soup.find('meta', attrs={'name': 'author'})
        if author_meta and author_meta.get('content'):
            author = author_meta['content'].strip()
            if author and len(author) < 30:
                return author

        return ""

    def extract_phone_number(self, html: str, soup: BeautifulSoup) -> str:
        """
        Extract phone number from content.

        Args:
            html: HTML content
            soup: BeautifulSoup object

        Returns:
            Phone number if found
        """
        # Japanese phone number patterns
        phone_patterns = [
            r'0[0-9]{1,4}-[0-9]{1,4}-[0-9]{3,4}',  # 03-1234-5678
            r'0[0-9]{9,10}',  # 0312345678
            r'\+81[0-9-]{10,}',  # +81-3-1234-5678
        ]

        # Check tel: links first
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('tel:'):
                phone = href.replace('tel:', '').strip()
                phone = re.sub(r'[^\d\-\+]', '', phone)
                if phone:
                    return phone[:20]

        # Search in HTML for phone patterns
        for pattern in phone_patterns:
            matches = re.findall(pattern, html)
            if matches:
                # Return first match
                return matches[0][:20]

        return ""

    def extract_address(self, html: str, visible_text: str) -> str:
        """
        Extract business address from content.

        Args:
            html: HTML content
            visible_text: Visible text

        Returns:
            Address if found
        """
        # Look for postal code + address pattern
        address_pattern = r'〒?\s*[0-9]{3}-?[0-9]{4}\s*([^\n]{10,50})'
        matches = re.findall(address_pattern, visible_text)
        if matches:
            return matches[0][:100].strip()

        # Look for city keywords with following text
        for city in CITY_KEYWORDS:
            if city in visible_text:
                # Extract text around city name
                idx = visible_text.find(city)
                if idx != -1:
                    # Get 50 chars before and after
                    start = max(0, idx - 20)
                    end = min(len(visible_text), idx + 80)
                    context = visible_text[start:end].strip()
                    # Clean and return
                    context = ' '.join(context.split())
                    return context[:100]

        return ""

    def extract_business_hours(self, visible_text: str) -> str:
        """
        Extract business hours from content.

        Args:
            visible_text: Visible text

        Returns:
            Business hours if found
        """
        # Look for common hour patterns
        hour_keywords = ['営業時間', '受付時間', '診療時間', 'open', 'hours']

        for keyword in hour_keywords:
            if keyword in visible_text:
                idx = visible_text.find(keyword)
                if idx != -1:
                    # Get text after keyword
                    start = idx
                    end = min(len(visible_text), idx + 100)
                    hours_text = visible_text[start:end]

                    # Extract time pattern (e.g., 10:00-19:00)
                    time_pattern = r'[0-9]{1,2}[:：][0-9]{2}\s*[-~〜]\s*[0-9]{1,2}[:：][0-9]{2}'
                    matches = re.findall(time_pattern, hours_text)
                    if matches:
                        return matches[0]

        return ""

    def classify_business_type(self, title: str, visible_text: str) -> str:
        """
        Classify the type of business.

        Args:
            title: Page title
            visible_text: Visible text

        Returns:
            Business type
        """
        combined_text = (title + ' ' + visible_text).lower()

        business_types = {
            'ヨガ': ['ヨガ', 'yoga'],
            'エステ': ['エステ', 'esthetic', 'エステティック'],
            '整体': ['整体', 'seitai', 'chiropractic'],
            '整骨': ['整骨', '接骨', 'sekkotsu'],
            'マッサージ': ['マッサージ', 'massage', 'リラクゼーション'],
            '鍼灸': ['鍼灸', '針', '灸', 'acupuncture'],
            'ネイル': ['ネイル', 'nail'],
            'ヘアサロン': ['ヘアサロン', '美容室', 'hair salon', '美容院'],
            'ピラティス': ['ピラティス', 'pilates'],
            'スピリチュアル': ['スピリチュアル', 'ヒーリング', 'リーディング', 'spiritual'],
        }

        for business_type, keywords in business_types.items():
            for keyword in keywords:
                if keyword in combined_text:
                    return business_type

        return '不明'

    def analyze(self, url: str, html: str, soup: BeautifulSoup, extracted_data: Dict) -> Dict:
        """
        Perform complete content analysis.

        Args:
            url: Page URL
            html: HTML content
            soup: BeautifulSoup object
            extracted_data: Data from crawler

        Returns:
            Analysis results including shop name, owner, phone, etc.
        """
        title = extracted_data.get('title', '')
        h1 = extracted_data.get('h1', '')
        visible_text = extracted_data.get('visible_text', '')

        # Check if aggregator
        is_aggregator = self.is_aggregator_site(title, html, visible_text)

        analysis = {
            'is_aggregator': is_aggregator,
            'shop_name': self.extract_shop_name(url, title, h1, soup, visible_text),
            'owner_name': self.extract_owner_name(html, soup, visible_text),
            'phone': self.extract_phone_number(html, soup),
            'address': self.extract_address(html, visible_text),
            'business_hours': self.extract_business_hours(visible_text),
            'business_type': self.classify_business_type(title, visible_text),
        }

        return analysis
