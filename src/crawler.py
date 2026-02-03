"""
Robust web crawler with retry logic, encoding detection, and content extraction.
"""
import re
import logging
import random
import requests
from typing import Optional, Dict
from bs4 import BeautifulSoup
import chardet
from src.utils.retry import exponential_backoff_retry
from config.settings import TIMEOUT, MAX_REDIRECTS, USER_AGENTS
from config.keywords import CITY_KEYWORDS

logger = logging.getLogger(__name__)


class WebCrawler:
    """Crawler for fetching and extracting website content."""

    def __init__(self):
        self.session = requests.Session()
        self.session.max_redirects = MAX_REDIRECTS

    def _get_random_user_agent(self) -> str:
        """Get a random user agent from the pool."""
        return random.choice(USER_AGENTS)

    @exponential_backoff_retry(max_retries=3, base_delay=1.0)
    def fetch(self, url: str) -> Optional[str]:
        """
        Fetch HTML content from URL with retry logic.

        Args:
            url: URL to fetch

        Returns:
            HTML content as string, or None if failed
        """
        headers = {
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
        }

        try:
            response = self.session.get(
                url,
                headers=headers,
                timeout=TIMEOUT,
                allow_redirects=True,
                verify=False  # Skip SSL verification for problematic sites
            )

            if response.status_code == 200:
                # Detect encoding if not specified
                if response.encoding == 'ISO-8859-1':
                    detected = chardet.detect(response.content)
                    if detected['encoding']:
                        response.encoding = detected['encoding']

                return response.text
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")
                return None

        except requests.exceptions.SSLError:
            logger.warning(f"SSL error for {url}, retrying without verification")
            # Already using verify=False, so this shouldn't happen
            return None
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching {url}")
            return None
        except requests.exceptions.TooManyRedirects:
            logger.warning(f"Too many redirects for {url}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def extract_data(self, url: str, html: str) -> Dict:
        """
        Extract relevant data from HTML content.

        Args:
            url: Original URL
            html: HTML content

        Returns:
            Dictionary with extracted data
        """
        soup = BeautifulSoup(html, 'html.parser')

        data = {
            'title': self._extract_title(soup),
            'h1': self._extract_h1(soup),
            'visible_text': self._extract_visible_text(soup),
            'contact_email': self._extract_email(html, soup),
            'city_guess': self._extract_city(html),
        }

        return data

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title."""
        if soup.title and soup.title.string:
            return soup.title.string.strip()[:200]
        return "Unknown"

    def _extract_h1(self, soup: BeautifulSoup) -> str:
        """Extract first H1 heading."""
        h1 = soup.find('h1')
        if h1:
            return h1.get_text(strip=True)[:200]
        return ""

    def _extract_visible_text(self, soup: BeautifulSoup) -> str:
        """Extract visible text content."""
        # Remove script and style elements
        for element in soup(['script', 'style', 'meta', 'link']):
            element.decompose()

        # Get text
        text = soup.get_text(separator=' ', strip=True)
        # Collapse whitespace
        text = ' '.join(text.split())
        return text[:5000]  # Limit to first 5000 chars

    def _extract_email(self, html: str, soup: BeautifulSoup) -> str:
        """Extract contact email from mailto links or text."""
        # Try mailto: links first
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('mailto:'):
                email = href.replace('mailto:', '').split('?')[0].strip()
                if email and '@' in email:
                    return email

        # Try regex on HTML content
        email_pattern = r'[\w\.-]+@[\w\.-]+\.\w+'
        matches = re.findall(email_pattern, html)

        # Filter out common false positives
        excluded = ['example@', 'test@', 'info@example', '@2x.png', '@gmail.png']
        for match in matches:
            if not any(ex in match.lower() for ex in excluded):
                return match

        return ""

    def _extract_city(self, html: str) -> str:
        """Extract Japanese city/ward name from content."""
        for city in CITY_KEYWORDS:
            if city in html:
                return city
        return ""

    def crawl(self, url: str) -> Optional[Dict]:
        """
        Fetch and extract data from URL.

        Args:
            url: URL to crawl

        Returns:
            Dictionary with extracted data, or None if failed
        """
        html = self.fetch(url)
        if not html:
            return None

        try:
            return self.extract_data(url, html)
        except Exception as e:
            logger.error(f"Error extracting data from {url}: {e}")
            return None
