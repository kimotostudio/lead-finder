"""
Multi-engine search coordinator for parallel searching across multiple engines.
"""
import logging
from typing import List, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.engines.duckduckgo import DuckDuckGoEngine
from src.engines.bing import BingEngine
from src.engines.brave import BraveEngine
from src.utils.url_filter import should_exclude_url, normalize_url

logger = logging.getLogger(__name__)


class MultiEngineSearch:
    """Coordinates searches across multiple search engines."""

    def __init__(self):
        self.engines = [
            DuckDuckGoEngine(),
            BingEngine(),
            BraveEngine(),
        ]
        # Filter to only available engines
        self.engines = [e for e in self.engines if e.is_available()]
        self.url_titles: dict = {}  # merged URL -> title from all engines
        logger.info(f"Initialized with {len(self.engines)} search engines: {[e.name for e in self.engines]}")

    def search(self, query: str, max_results_per_engine: int = 20) -> List[str]:
        """
        Search across all engines in parallel and combine results.

        Args:
            query: Search query
            max_results_per_engine: Max results per engine

        Returns:
            Combined, deduplicated list of URLs
        """
        all_urls = []
        seen_normalized = set()

        # Search engines in parallel
        with ThreadPoolExecutor(max_workers=len(self.engines)) as executor:
            future_to_engine = {
                executor.submit(engine.search, query, max_results_per_engine): engine
                for engine in self.engines
            }

            for future in as_completed(future_to_engine):
                engine = future_to_engine[future]
                try:
                    urls = future.result()
                    logger.info(f"{engine.name} returned {len(urls)} URLs for: {query}")

                    # Merge title side-channel from engine
                    self.url_titles.update(engine.url_titles)

                    # Filter and deduplicate
                    for url in urls:
                        # Exclude unwanted domains
                        if should_exclude_url(url):
                            continue

                        # Deduplicate by normalized URL
                        normalized = normalize_url(url)
                        if normalized not in seen_normalized:
                            seen_normalized.add(normalized)
                            all_urls.append(url)

                except Exception as e:
                    logger.error(f"{engine.name} failed for query '{query}': {e}")

        logger.info(f"Total unique URLs after filtering: {len(all_urls)} for query: {query}")

        return all_urls
