"""
Heuristic scoring system for evaluating website quality and improvement potential.
"""
import logging
from typing import Dict, Tuple
from urllib.parse import urlparse
from config.keywords import (
    FREE_PLATFORMS, PRICING_KEYWORDS, BOOKING_KEYWORDS,
    ACCESS_KEYWORDS, PROFILE_KEYWORDS
)
from config.settings import GRADE_A_THRESHOLD, GRADE_B_THRESHOLD

logger = logging.getLogger(__name__)


class WebsiteScorer:
    """Scores websites based on heuristics for improvement potential."""

    def _detect_site_type(self, url: str, html: str) -> str:
        """
        Detect the type of site builder/platform used.

        Args:
            url: Website URL
            html: HTML content

        Returns:
            Site type name (e.g., 'peraichi', 'wordpress', 'custom')
        """
        url_lower = url.lower()
        html_lower = html.lower()

        # Check against known free platforms
        for platform, patterns in FREE_PLATFORMS.items():
            for pattern in patterns:
                if pattern in url_lower or pattern in html_lower:
                    return platform

        return 'custom'

    def _check_keyword_presence(self, text: str, keywords: list) -> bool:
        """Check if any keyword is present in text."""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in keywords)

    def _is_https(self, url: str) -> bool:
        """Check if URL uses HTTPS."""
        return url.lower().startswith('https://')

    def _has_modern_design_indicators(self, html: str) -> bool:
        """
        Check for modern design indicators (responsive meta, CSS frameworks).

        Args:
            html: HTML content

        Returns:
            True if modern design detected
        """
        html_lower = html.lower()

        indicators = [
            'viewport',  # Responsive meta tag
            'bootstrap',  # Bootstrap CSS
            'tailwind',   # Tailwind CSS
            'material-ui',  # Material UI
            'foundation',  # Foundation CSS
        ]

        return any(indicator in html_lower for indicator in indicators)

    def score(self, url: str, html: str, extracted_data: Dict) -> Dict:
        """
        Calculate heuristic score and grade for a website.

        Args:
            url: Website URL
            html: HTML content
            extracted_data: Extracted data from crawler

        Returns:
            Dictionary with score, grade, reasons, and site_type
        """
        score = 0
        reasons = []

        # Detect site type
        site_type = self._detect_site_type(url, html)

        # Rule 1: Free site builder (+40 points)
        if site_type in FREE_PLATFORMS:
            score += 40
            reasons.append(site_type)

        # Rule 2: HTTP only (+15 points)
        if not self._is_https(url):
            score += 15
            reasons.append('http_only')

        visible_text = extracted_data.get('visible_text', '')

        # Rule 3: No pricing info (+10 points)
        if not self._check_keyword_presence(visible_text, PRICING_KEYWORDS):
            score += 10
            reasons.append('no_pricing')

        # Rule 4: No booking system (+10 points)
        if not self._check_keyword_presence(visible_text, BOOKING_KEYWORDS):
            score += 10
            reasons.append('no_booking')

        # Rule 5: No access/location info (+10 points)
        if not self._check_keyword_presence(visible_text, ACCESS_KEYWORDS):
            score += 10
            reasons.append('no_access')

        # Rule 6: No profile/about (+10 points)
        if not self._check_keyword_presence(visible_text, PROFILE_KEYWORDS):
            score += 10
            reasons.append('no_profile')

        # Rule 7: SNS-only redirect (+15 points)
        # Check if content is minimal (likely redirect page)
        if len(visible_text) < 500 and any(
            sns in html.lower() for sns in ['instagram.com', 'facebook.com', 'twitter.com', 'x.com']
        ):
            score += 15
            reasons.append('sns_redirect')

        # Rule 8: Modern design penalty (-20 points)
        # If site is custom domain with modern design, reduce score
        if site_type == 'custom' and self._has_modern_design_indicators(html):
            score = max(0, score - 20)
            reasons.append('modern_design')

        # Cap at 100
        score = min(score, 100)

        # Assign grade
        if score >= GRADE_A_THRESHOLD:
            grade = 'A'
        elif score >= GRADE_B_THRESHOLD:
            grade = 'B'
        else:
            grade = 'C'

        return {
            'score': score,
            'grade': grade,
            'reasons': '; '.join(reasons) if reasons else '',
            'site_type': site_type,
        }
