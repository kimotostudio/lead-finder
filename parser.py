"""
HTML parser to extract website fields.
"""

import re
import logging
from urllib.parse import urlparse
from typing import Dict
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Japanese city/ward patterns
JAPANESE_CITIES = [
    '東京都', '大阪府', '京都府', '北海道',
    '横浜市', '名古屋市', '札幌市', '神戸市', '福岡市', '川崎市',
    '千葉市', '仙台市', '広島市', '北九州市', 'さいたま市',
    '渋谷区', '新宿区', '港区', '世田谷区', '品川区', '目黒区',
    '大田区', '中野区', '杉並区', '豊島区', '北区', '板橋区',
    '練馬区', '足立区', '葛飾区', '江戸川区', '千代田区', '中央区',
    '文京区', '台東区', '墨田区', '江東区', '荒川区',
]


def extract_name(soup: BeautifulSoup) -> str:
    """Extract business name from title or H1."""
    # Try title first
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
        if title:
            return title[:100]  # Limit length

    # Try first H1
    h1 = soup.find('h1')
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text[:100]

    return "Unknown"


def extract_email(html_content: str, soup: BeautifulSoup) -> str:
    """Extract contact email from mailto links or regex."""
    # Try mailto: links first
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if href.startswith('mailto:'):
            email = href.replace('mailto:', '').split('?')[0].strip()
            if email:
                return email

    # Regex pattern for emails in text
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    matches = re.findall(email_pattern, html_content)

    if matches:
        return matches[0]

    return ""


def extract_city(html_content: str) -> str:
    """Detect Japanese city/ward names in content."""
    for city in JAPANESE_CITIES:
        if city in html_content:
            return city
    return ""


def classify_site_type(url: str, html_content: str) -> str:
    """
    Classify website by platform/builder.

    Free platforms: peraichi, crayonsite, jimdo, wix, ameblo, fc2, note,
                    studio.site, lit.link, linktr.ee, thebase, wordpress
    """
    domain_lower = url.lower()
    html_lower = html_content.lower()

    # Check domain patterns
    if 'peraichi.com' in domain_lower:
        return 'peraichi'
    if 'crayonsite.info' in domain_lower or 'crayonsite.net' in domain_lower:
        return 'crayon'
    if 'jimdo' in domain_lower:
        return 'jimdo'
    if 'wixsite.com' in domain_lower or 'wix.com' in domain_lower:
        return 'wix'
    if 'ameblo.jp' in domain_lower or 'ameba.jp' in domain_lower:
        return 'ameblo'
    if 'fc2.com' in domain_lower:
        return 'fc2'
    if 'note.com' in domain_lower:
        return 'note'
    if 'studio.site' in domain_lower:
        return 'studio.site'
    if 'lit.link' in domain_lower:
        return 'lit.link'
    if 'linktr.ee' in domain_lower:
        return 'linktree'
    if 'thebase.in' in domain_lower:
        return 'thebase'

    # Check for WordPress
    if 'wp-content' in html_lower or 'wordpress' in html_lower:
        return 'wordpress'

    return 'custom'


def parse_website_data(url: str, html_content: str) -> Dict:
    """
    Parse website HTML and extract all fields.

    Returns dict with: name, url, domain, site_type, contact_email, city_guess
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract domain
    parsed_url = urlparse(url)
    domain = parsed_url.netloc

    # Extract fields
    name = extract_name(soup)
    contact_email = extract_email(html_content, soup)
    city_guess = extract_city(html_content)
    site_type = classify_site_type(url, html_content)

    return {
        'name': name,
        'url': url,
        'domain': domain,
        'site_type': site_type,
        'contact_email': contact_email,
        'city_guess': city_guess,
    }
