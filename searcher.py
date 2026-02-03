"""
Search scraper for collecting URLs from queries.

Uses ddgs library for reliable search results.
Implements rate limiting to be respectful to the service.
"""

import logging
import time
from typing import List
from ddgs import DDGS

logger = logging.getLogger(__name__)


def search_urls_for_query(query: str, max_results: int = 10) -> List[str]:
    """
    Search for URLs using duckduckgo-search library.

    Args:
        query: Search query string (supports Japanese)
        max_results: Maximum URLs to return (default: 10)

    Returns:
        List of URLs found (duplicates removed)
    """
    urls = []

    try:
        # Use DDGS for search (without context manager for better compatibility)
        ddgs = DDGS()
        results = list(ddgs.text(
            query=query,
            region='jp-jp',  # Japan region for Japanese queries
            safesearch='off',
            max_results=max_results
        ))

        # Extract URLs from results and filter out non-target sites
        # Block: Chinese sites, major portals, aggregators, blogs, ads
        blocked_domains = [
            'zhihu.com', 'baidu.com', 'weibo.com', 'qq.com',  # Chinese sites
            'hotpepper.jp', 'rakuten.co.jp', 'ameblo.jp', 'ameba.jp',  # Major portals
            'wikipedia.org', 'yahoo.co.jp', 'google.com', 'bing.com',  # Info sites & ads
            'epark.jp', 'ekiten.jp', 'navitime.co.jp',  # Portal/review sites
            'mitsuraku.jp', 'ozmall.co.jp', 'beauty-park.jp',  # Beauty portals
            'judo-ch.jp', 'karadarefre.jp', 'raku-navi.jp',  # Health portals
            'health-more.jp', 'rairai.net', 'shuminavi.net',  # Portal sites
            'note.com', 'fc2.com',  # Blog platforms
            'zehitomo.com', 'street-academy.com',  # Marketplace
            '.cn/', '.mom/', 'shuhaixsw.com', '51hlw5.com',  # Chinese domains
        ]
        blocked_count = 0
        for result in results:
            if 'href' in result:
                url = result['href']
                # Skip blocked domains
                if any(domain in url.lower() for domain in blocked_domains):
                    blocked_count += 1
                    logger.debug(f"Blocked: {url}")
                    continue
                if url not in urls:  # Remove duplicates
                    urls.append(url)
                    logger.debug(f"Added: {url}")

        if blocked_count > 0:
            logger.info(f"Blocked {blocked_count} Chinese sites")

        # Rate limiting: 1 second delay between requests
        time.sleep(1.0)

        logger.info(f"Found {len(urls)} unique URLs for query: {query}")

    except Exception as e:
        logger.error(f"Search error for query '{query}': {e}")

    return urls
