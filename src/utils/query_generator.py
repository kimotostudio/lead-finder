"""
Query variation generator for comprehensive search coverage.
"""
import logging
from typing import List
from config.keywords import QUERY_SUFFIXES, SITE_TYPE_SUFFIXES

logger = logging.getLogger(__name__)


def generate_query_variations(base_query: str, include_site_types: bool = True) -> List[str]:
    """
    Generate variations of a base search query.

    Args:
        base_query: Base search query (e.g., "渋谷 ヨガ")
        include_site_types: Whether to include site type variations

    Returns:
        List of query variations
    """
    variations = []

    # Add base query with suffixes
    for suffix in QUERY_SUFFIXES:
        if suffix:
            variations.append(f"{base_query} {suffix}")
        else:
            variations.append(base_query)

    # Add site type targeted queries if requested
    if include_site_types:
        for site_type in SITE_TYPE_SUFFIXES:
            variations.append(f"{base_query} {site_type}")

    # Remove duplicates while preserving order
    seen = set()
    unique_variations = []
    for query in variations:
        query_clean = query.strip()
        if query_clean and query_clean not in seen:
            seen.add(query_clean)
            unique_variations.append(query_clean)

    logger.info(f"Generated {len(unique_variations)} variations for query: {base_query}")

    return unique_variations


def load_queries_from_file(filepath: str) -> List[str]:
    """
    Load queries from text file (one per line).

    Args:
        filepath: Path to queries file

    Returns:
        List of queries
    """
    queries = []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    queries.append(line)

        logger.info(f"Loaded {len(queries)} base queries from {filepath}")

    except Exception as e:
        logger.error(f"Failed to load queries from {filepath}: {e}")

    return queries
