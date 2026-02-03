"""
Lead deduplication logic.
"""

import logging
from typing import List, Dict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def normalize_url(url: str) -> str:
    """
    Normalize URL for deduplication.

    Uses domain + path (without query params/fragments).
    """
    parsed = urlparse(url)
    # Combine domain and path, remove trailing slash
    normalized = f"{parsed.netloc}{parsed.path}".rstrip('/')
    return normalized.lower()


def deduplicate_leads(leads: List[Dict]) -> List[Dict]:
    """
    Deduplicate leads by normalized URL.

    Strategy: Keep the first occurrence of each unique normalized URL.
    This preserves the highest scored item if sorted.
    """
    seen_urls = set()
    unique_leads = []

    for lead in leads:
        url = lead['url']
        normalized = normalize_url(url)

        if normalized not in seen_urls:
            seen_urls.add(normalized)
            unique_leads.append(lead)
        else:
            logger.debug(f"Duplicate filtered: {url}")

    return unique_leads
