"""
Japanese content detection for lead filtering.

Provides URL-level and title-level Japanese content detection
to filter out overseas/irrelevant sites early in the pipeline.
"""
import re
from urllib.parse import urlparse
from typing import Tuple

# Pre-compiled regex patterns for Japanese character detection
_RE_HIRAGANA = re.compile(r'[\u3040-\u309F]')
_RE_KATAKANA = re.compile(r'[\u30A0-\u30FF]')
_RE_KANJI = re.compile(r'[\u4E00-\u9FFF]')
_RE_JP_CHAR = re.compile(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]')

# Domains that are definitely overseas (never JP business sites)
OVERSEAS_DOMAINS = {
    'yelp.com', 'yelp.co.uk', 'yelp.de', 'yelp.fr',
    'tripadvisor.com', 'tripadvisor.co.uk', 'tripadvisor.de',
    'booking.com', 'expedia.com', 'hotels.com', 'airbnb.com',
    'facebook.com', 'instagram.com', 'linkedin.com',
    'twitter.com', 'x.com', 'pinterest.com', 'tiktok.com',
    'reddit.com', 'quora.com', 'medium.com',
    'trustpilot.com', 'glassdoor.com',
    'thumbtack.com', 'angi.com', 'homeadvisor.com',
    'healthgrades.com', 'zocdoc.com', 'webmd.com', 'mayoclinic.org',
    'craigslist.org', 'gumtree.com',
    'wikipedia.org', 'wikihow.com',
    'youtube.com', 'vimeo.com',
    'amazon.com', 'ebay.com', 'etsy.com',
    'groupon.com', 'yelp.ca',
}

# Foreign TLDs (non-Japanese country-code domains)
FOREIGN_TLDS = {
    '.de', '.fr', '.es', '.it', '.nl', '.pt', '.pl', '.ru', '.se',
    '.no', '.dk', '.fi', '.cz', '.at', '.ch', '.be', '.ie', '.gr',
    '.hu', '.ro', '.bg', '.hr', '.sk', '.si', '.lt', '.lv', '.ee',
    '.co.uk', '.org.uk', '.me.uk',
    '.com.au', '.com.br', '.com.mx', '.com.ar', '.com.co',
    '.com.tr', '.com.sa', '.com.eg', '.co.za', '.co.ke',
    '.co.in', '.co.id', '.co.th', '.co.kr', '.com.cn', '.com.tw',
    '.com.hk', '.com.sg', '.com.my', '.com.ph', '.com.vn',
    '.in', '.cn', '.kr', '.tw', '.th', '.vn', '.ph', '.my', '.sg', '.id',
}

# Known Japanese platforms (non-.jp domains that host JP businesses)
JP_PLATFORM_DOMAINS = {
    'peraichi.com', 'jimdofree.com', 'wixsite.com', 'wordpress.com',
    'studio.site', 'crayonsite.com', 'crayonsite.net',
    'goope.jp', 'stores.jp', 'base.shop', 'booth.pm',
    'jimdo.com', 'wix.com', 'squarespace.com', 'webnode.jp',
    'weebly.com', 'strikingly.com', 'thebase.in', 'shopify.com',
}


def has_japanese_characters(text: str) -> bool:
    """Check if text contains any Japanese characters (hiragana, katakana, or kanji)."""
    if not text:
        return False
    return bool(
        _RE_HIRAGANA.search(text) or
        _RE_KATAKANA.search(text) or
        _RE_KANJI.search(text)
    )


def japanese_char_ratio(text: str) -> float:
    """Return the fraction of characters in text that are Japanese."""
    if not text:
        return 0.0
    total = len(text)
    jp_count = len(_RE_JP_CHAR.findall(text))
    return jp_count / total


def is_japanese_url(url: str) -> bool:
    """
    Check if URL is positively Japanese based on domain patterns.
    Returns True for .jp domains, known JP platforms, and /ja/ paths.
    """
    try:
        parsed = urlparse(url.lower())
        domain = parsed.netloc.replace('www.', '')

        # .jp domains (all variants)
        if domain.endswith('.jp'):
            return True

        # Known JP platforms
        for platform in JP_PLATFORM_DOMAINS:
            if domain == platform or domain.endswith('.' + platform):
                return True

        # /ja/ path segment
        if '/ja/' in parsed.path:
            return True

        return False
    except Exception:
        return False


def is_definitely_overseas_url(url: str) -> bool:
    """
    Check if URL is definitely overseas (non-Japanese).
    Returns True for known overseas domains and foreign TLDs.
    """
    try:
        parsed = urlparse(url.lower())
        domain = parsed.netloc.replace('www.', '')

        # Never block .jp domains
        if domain.endswith('.jp'):
            return False

        # Check overseas-only domains
        for overseas in OVERSEAS_DOMAINS:
            if domain == overseas or domain.endswith('.' + overseas):
                return True

        # Check foreign TLDs
        for tld in FOREIGN_TLDS:
            if domain.endswith(tld):
                return True

        return False
    except Exception:
        return False


def estimate_japanese_from_title(title: str, threshold: float = 0.3) -> bool:
    """
    Estimate if a page is Japanese based on its search result title.
    Returns True if Japanese character ratio >= threshold.
    """
    if not title:
        return False
    ratio = japanese_char_ratio(title)
    return ratio >= threshold


def classify_url_japanese(url: str, title: str = '') -> str:
    """
    Classify a URL as 'japanese', 'overseas', or 'uncertain'.

    Logic:
    1. If URL is definitely overseas → 'overseas'
    2. If URL is positively Japanese → 'japanese'
    3. If title has ≥30% Japanese characters → 'japanese'
    4. Otherwise → 'uncertain' (kept for further processing)
    """
    # Definitely overseas? Block
    if is_definitely_overseas_url(url):
        return 'overseas'

    # Positively Japanese URL? Keep
    if is_japanese_url(url):
        return 'japanese'

    # Title-based detection for .com/.net/.org URLs
    if title and estimate_japanese_from_title(title):
        return 'japanese'

    return 'uncertain'
