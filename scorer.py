"""
Heuristic scoring engine for website improvement potential.
"""

import re
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

# Free platform types that get +40 points
FREE_PLATFORMS = {
    'peraichi', 'crayon', 'jimdo', 'wix', 'ameblo',
    'fc2', 'note', 'studio.site', 'lit.link', 'linktree', 'thebase', 'wordpress'
}


def check_keyword_presence(html_content: str, keywords: List[str]) -> bool:
    """Check if any keyword is present in HTML content."""
    html_lower = html_content.lower()
    return any(keyword in html_lower for keyword in keywords)


def is_sns_only_site(html_content: str) -> bool:
    """
    Detect if site appears to be SNS-only or minimal content.

    Criteria: Contains instagram/facebook/twitter links and very little text.
    """
    html_lower = html_content.lower()

    # Check for SNS presence
    has_sns = any(platform in html_lower for platform in [
        'instagram.com', 'facebook.com', 'twitter.com', 'x.com'
    ])

    if not has_sns:
        return False

    # Check text content length (rough estimate)
    # Remove HTML tags and count characters
    text_content = re.sub(r'<[^>]+>', '', html_content)
    text_content = re.sub(r'\s+', ' ', text_content).strip()

    # If very short (< 500 chars) with SNS links, likely SNS-only
    if len(text_content) < 500:
        return True

    return False


def score_website(parsed_data: Dict, html_content: str, url: str) -> Dict:
    """
    Calculate heuristic score (0-100) and grade (A/B/C).

    Scoring rules:
    - +40 if free builder/blog platform
    - +10 if not HTTPS
    - +10 if no pricing info
    - +10 if no booking info
    - +10 if no access/location info
    - +10 if no profile info
    - +15 if SNS-only or minimal

    Grade: A >= 60, B >= 40, C < 40

    Returns dict with: score, grade, reasons
    """
    score = 0
    reasons = []

    site_type = parsed_data['site_type']

    # Rule 1: Free platform (+40)
    if site_type in FREE_PLATFORMS:
        score += 40
        reasons.append(site_type)

    # Rule 2: Not HTTPS (+10)
    if not url.startswith('https://'):
        score += 10
        reasons.append('http_only')

    # Rule 3: No pricing (+10)
    pricing_keywords = ['料金', 'price', 'menu', '価格', 'プラン']
    if not check_keyword_presence(html_content, pricing_keywords):
        score += 10
        reasons.append('no_pricing')

    # Rule 4: No booking (+10)
    booking_keywords = ['予約', 'booking', 'カレンダー', 'reservation', 'ご予約']
    if not check_keyword_presence(html_content, booking_keywords):
        score += 10
        reasons.append('no_booking')

    # Rule 5: No access/location (+10)
    access_keywords = ['アクセス', '住所', '所在地', 'map', 'address', '地図']
    if not check_keyword_presence(html_content, access_keywords):
        score += 10
        reasons.append('no_access')

    # Rule 6: No profile (+10)
    profile_keywords = ['プロフィール', '自己紹介', 'about', 'profile']
    if not check_keyword_presence(html_content, profile_keywords):
        score += 10
        reasons.append('no_profile')

    # Rule 7: SNS-only or minimal (+15)
    if is_sns_only_site(html_content):
        score += 15
        reasons.append('sns_only')

    # Cap at 100
    score = min(score, 100)

    # Assign grade
    if score >= 60:
        grade = 'A'
    elif score >= 40:
        grade = 'B'
    else:
        grade = 'C'

    # Format reasons
    reasons_str = '; '.join(reasons) if reasons else ''

    return {
        'score': score,
        'grade': grade,
        'reasons': reasons_str,
    }
