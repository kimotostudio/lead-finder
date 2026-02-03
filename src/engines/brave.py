"""
Brave search engine implementation (requires API key).
"""
import time
import requests
from typing import List
from src.engines.base import SearchEngine
from src.utils.retry import exponential_backoff_retry
from config.settings import BRAVE_API_KEY, RATE_LIMIT_DELAY, TIMEOUT


class BraveEngine(SearchEngine):
    """Brave search engine using Brave Search API."""

    def __init__(self):
        super().__init__("Brave")
        self.api_key = BRAVE_API_KEY
        self.endpoint = "https://api.search.brave.com/res/v1/web/search"

    def is_available(self) -> bool:
        """Check if Brave API key is configured."""
        return bool(self.api_key)

    @exponential_backoff_retry(max_retries=3, base_delay=2.0)
    def search(self, query: str, max_results: int = 20) -> List[str]:
        """
        Search using Brave API.

        Args:
            query: Search query
            max_results: Maximum results to return

        Returns:
            List of URLs
        """
        if not self.is_available():
            self.logger.warning("Brave API key not configured, skipping")
            return []

        urls = []

        try:
            headers = {
                "Accept": "application/json",
                "X-Subscription-Token": self.api_key
            }
            params = {
                "q": query,
                "count": min(max_results, 20),  # Brave max is 20
                "country": "JP",
                "search_lang": "ja",
            }

            response = requests.get(
                self.endpoint,
                headers=headers,
                params=params,
                timeout=TIMEOUT
            )
            response.raise_for_status()

            data = response.json()

            if 'web' in data and 'results' in data['web']:
                for item in data['web']['results']:
                    if 'url' in item:
                        url = item['url']
                        urls.append(url)
                        self.url_titles[url] = item.get('title', '')

            self.logger.info(f"Brave found {len(urls)} URLs for: {query}")

            # Rate limiting
            time.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            self.logger.error(f"Brave search error for '{query}': {e}")
            raise

        return urls
