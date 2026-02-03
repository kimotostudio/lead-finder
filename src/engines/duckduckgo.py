"""
DuckDuckGo search engine implementation.
"""
import time
from typing import List
from ddgs import DDGS
from src.engines.base import SearchEngine
from src.utils.retry import exponential_backoff_retry
from config.settings import RATE_LIMIT_DELAY


class DuckDuckGoEngine(SearchEngine):
    """DuckDuckGo search engine using ddgs library."""

    def __init__(self):
        super().__init__("DuckDuckGo")

    @exponential_backoff_retry(max_retries=3, base_delay=2.0)
    def search(self, query: str, max_results: int = 20) -> List[str]:
        """
        Search using DuckDuckGo.

        Args:
            query: Search query
            max_results: Maximum results to return

        Returns:
            List of URLs
        """
        urls = []

        try:
            ddgs = DDGS()
            results = list(ddgs.text(
                query=query,
                region='jp-jp',
                safesearch='off',
                max_results=max_results
            ))

            for result in results:
                if 'href' in result:
                    url = result['href']
                    urls.append(url)
                    self.url_titles[url] = result.get('title', '')

            self.logger.info(f"DuckDuckGo found {len(urls)} URLs for: {query}")

            # Rate limiting
            time.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            self.logger.error(f"DuckDuckGo search error for '{query}': {e}")
            raise

        return urls
