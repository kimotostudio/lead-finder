#!/usr/bin/env python3
"""
Lead Finder & Website Scorer
Finds small-business leads and scores website improvement potential using heuristics.
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Set
from urllib.parse import urlparse, urljoin

from fetcher import fetch_url
from parser import parse_website_data
from scorer import score_website
from searcher import search_urls_for_query
from deduplicator import deduplicate_leads

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_queries(filepath: str) -> List[str]:
    """Load search queries from file, one per line."""
    queries = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    queries.append(line)
        logger.info(f"Loaded {len(queries)} queries from {filepath}")
        return queries
    except Exception as e:
        logger.error(f"Failed to load queries from {filepath}: {e}")
        return []


def load_urls(filepath: str) -> List[str]:
    """Load URLs from file, one per line."""
    urls = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    urls.append(line)
        logger.info(f"Loaded {len(urls)} URLs from {filepath}")
        return urls
    except Exception as e:
        logger.error(f"Failed to load URLs from {filepath}: {e}")
        return []


def collect_urls_from_queries(queries: List[str]) -> List[str]:
    """Collect candidate URLs from search queries."""
    all_urls = []
    for query in queries:
        logger.info(f"Searching for: {query}")
        try:
            urls = search_urls_for_query(query, max_results=10)
            all_urls.extend(urls)
            logger.info(f"  Found {len(urls)} URLs")
        except Exception as e:
            logger.error(f"  Search failed: {e}")

    return all_urls


def process_url(url: str) -> Dict:
    """Process a single URL: fetch, parse, and score."""
    logger.info(f"Processing: {url}")

    # Fetch HTML
    html_content = fetch_url(url)
    if not html_content:
        logger.warning(f"  Failed to fetch {url}")
        return None

    # Parse website data
    try:
        parsed_data = parse_website_data(url, html_content)

        # Score the website
        score_data = score_website(parsed_data, html_content, url)

        # Merge data
        result = {**parsed_data, **score_data}

        logger.info(f"  Score: {result['score']} ({result['grade']}) - {result['site_type']}")
        return result

    except Exception as e:
        logger.error(f"  Error processing {url}: {e}")
        return None


def save_to_csv(leads: List[Dict], output_path: str):
    """Save leads to CSV with exact column order."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    columns = [
        'name', 'url', 'domain', 'site_type',
        'score', 'grade', 'reasons', 'contact_email', 'city_guess'
    ]

    try:
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(leads)

        logger.info(f"Saved {len(leads)} leads to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Find small-business leads and score website improvement potential'
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--queries', help='Path to queries.txt file')
    group.add_argument('--urls', help='Path to urls.txt file')

    parser.add_argument(
        '--output',
        default='output/leads.csv',
        help='Output CSV path (default: output/leads.csv)'
    )

    parser.add_argument(
        '--max',
        type=int,
        help='Maximum number of candidate URLs to process (optional)'
    )

    args = parser.parse_args()

    # Collect URLs
    candidate_urls = []

    if args.queries:
        if not os.path.exists(args.queries):
            logger.error(f"Queries file not found: {args.queries}")
            sys.exit(1)

        queries = load_queries(args.queries)
        if not queries:
            logger.error("No queries loaded")
            sys.exit(1)

        candidate_urls = collect_urls_from_queries(queries)

    # Limit number of candidate URLs if requested
    if args.max and args.max > 0:
        candidate_urls = candidate_urls[: args.max]

    elif args.urls:
        if not os.path.exists(args.urls):
            logger.error(f"URLs file not found: {args.urls}")
            sys.exit(1)

        candidate_urls = load_urls(args.urls)

    if not candidate_urls:
        logger.error("No URLs to process")
        sys.exit(1)

    logger.info(f"Total candidate URLs: {len(candidate_urls)}")

    # Process each URL
    leads = []
    for url in candidate_urls:
        result = process_url(url)
        if result:
            leads.append(result)

    if not leads:
        logger.warning("No successful leads processed")
        sys.exit(0)

    # Deduplicate
    logger.info(f"Leads before deduplication: {len(leads)}")
    leads = deduplicate_leads(leads)
    logger.info(f"Leads after deduplication: {len(leads)}")

    # Sort by score (highest first)
    leads.sort(key=lambda x: x['score'], reverse=True)

    # Save to CSV
    save_to_csv(leads, args.output)

    # Summary
    grade_counts = {}
    for lead in leads:
        grade = lead['grade']
        grade_counts[grade] = grade_counts.get(grade, 0) + 1

    logger.info("=== Summary ===")
    logger.info(f"Total leads: {len(leads)}")
    for grade in ['A', 'B', 'C']:
        count = grade_counts.get(grade, 0)
        logger.info(f"  Grade {grade}: {count}")


if __name__ == '__main__':
    main()
