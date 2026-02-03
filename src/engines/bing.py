"""
Bing search engine implementation (requires API key).
"""
import time
import requests
from typing import List
from src.engines.base import SearchEngine
from src.utils.retry import exponential_backoff_retry
from config.settings import BING_API_KEY, RATE_LIMIT_DELAY, TIMEOUT


class BingEngine(SearchEngine):
    """Bing search engine using Bing Web Search API."""

    def __init__(self):
        super().__init__("Bing")
        self.api_key = BING_API_KEY
        self.endpoint = "https://api.bing.microsoft.com/v7.0/search"

    def is_available(self) -> bool:
        """Check if Bing API key is configured."""
        return bool(self.api_key)

    @exponential_backoff_retry(max_retries=3, base_delay=2.0)
    def search(self, query: str, max_results: int = 20) -> List[str]:
        """
        Search using Bing API.

        Args:
            query: Search query
            max_results: Maximum results to return

        Returns:
            List of URLs
        """
        if not self.is_available():
            self.logger.warning("Bing API key not configured, skipping")
            return []

        urls = []

        try:
            headers = {"Ocp-Apim-Subscription-Key": self.api_key}
            params = {
                "q": query,
                "count": min(max_results, 50),  # Bing max is 50
                "mkt": "ja-JP",
                "textDecorations": False,
                "textFormat": "Raw"
            }

            response = requests.get(
                self.endpoint,
                headers=headers,
                params=params,
                timeout=TIMEOUT
            )
            response.raise_for_status()

            data = response.json()

            if 'webPages' in data and 'value' in data['webPages']:
                for item in data['webPages']['value']:
                    if 'url' in item:
                        url = item['url']
                        urls.append(url)
                        self.url_titles[url] = item.get('name', '')

            self.logger.info(f"Bing found {len(urls)} URLs for: {query}")

            # Rate limiting
            time.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            self.logger.error(f"Bing search error for '{query}': {e}")
            raise

        return urls
