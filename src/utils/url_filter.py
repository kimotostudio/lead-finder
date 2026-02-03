"""
URL filtering utilities to exclude unwanted domains and prioritize target sites.
"""
import logging
from urllib.parse import urlparse
from config.keywords import EXCLUDED_DOMAINS, PRIORITIZED_PATTERNS

logger = logging.getLogger(__name__)


def should_exclude_url(url: str) -> bool:
    """
    Check if URL should be excluded based on domain patterns.

    Args:
        url: URL to check

    Returns:
        True if URL should be excluded, False otherwise
    """
    url_lower = url.lower()

    for pattern in EXCLUDED_DOMAINS:
        if pattern in url_lower:
            logger.debug(f"Excluding URL (matched '{pattern}'): {url}")
            return True

    return False


def get_priority_score(url: str) -> int:
    """
    Get priority score for URL based on domain patterns.
    Higher score = higher priority.

    Args:
        url: URL to score

    Returns:
        Priority score (0-100)
    """
    url_lower = url.lower()

    # Check for prioritized patterns
    for pattern in PRIORITIZED_PATTERNS:
        if pattern in url_lower:
            return 100

    # Custom domains (.com, .jp, .net) with short paths get medium priority
    parsed = urlparse(url)
    if parsed.netloc and not any(p in url_lower for p in ['.blogspot.', '.wordpress.com', '.tumblr.']):
        # Check if it's a custom domain (not a subdomain of major platforms)
        if len(parsed.path.split('/')) <= 3:  # Short path suggests main site
            return 50

    return 10


def normalize_url(url: str) -> str:
    """
    Normalize URL for deduplication.
    Removes www, trailing slashes, query params, fragments.

    Args:
        url: URL to normalize

    Returns:
        Normalized URL
    """
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(url)

    # Remove www prefix
    netloc = parsed.netloc.lower()
    if netloc.startswith('www.'):
        netloc = netloc[4:]

    # Remove trailing slash from path
    path = parsed.path.rstrip('/')

    # Reconstruct without query/fragment
    normalized = urlunparse((
        parsed.scheme.lower(),
        netloc,
        path,
        '',  # params
        '',  # query
        ''   # fragment
    ))

    return normalized


def extract_domain(url: str) -> str:
    """
    Extract domain from URL.

    Args:
        url: URL to extract domain from

    Returns:
        Domain name
    """
    parsed = urlparse(url)
    return parsed.netloc.lower()
