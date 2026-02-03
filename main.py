"""
Main application entry point for production lead finder system.
"""
import argparse
import logging
import sys
from pathlib import Path
from src.engines.multi_engine import MultiEngineSearch
from src.processor import LeadProcessor
from src.output_writer import OutputWriter
from src.utils.query_generator import load_queries_from_file, generate_query_variations
from config.settings import MAX_RESULTS_PER_QUERY, PARALLEL_WORKERS


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/lead_finder.log', encoding='utf-8')
        ]
    )

    # Suppress noisy libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)


def load_urls_from_file(filepath: str):
    """Load URLs from text file."""
    urls = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    urls.append(line)
        logging.info(f"Loaded {len(urls)} URLs from {filepath}")
    except Exception as e:
        logging.error(f"Failed to load URLs from {filepath}: {e}")
    return urls


def main():
    """Main application logic."""
    parser = argparse.ArgumentParser(
        description='Production-ready lead finder for small businesses',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --queries data/queries.txt --output output/leads.csv
  python main.py --urls data/urls.txt --output output/leads.csv
  python main.py --queries data/queries.txt --limit 50 --parallel 20 --verbose
        """
    )

    # Input source (mutually exclusive)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--queries',
        help='Path to queries file (one query per line)'
    )
    group.add_argument(
        '--urls',
        help='Path to URLs file (one URL per line)'
    )

    # Output settings
    parser.add_argument(
        '--output',
        default='output/leads.csv',
        help='Output CSV path (default: output/leads.csv)'
    )

    # Processing settings
    parser.add_argument(
        '--limit',
        type=int,
        default=MAX_RESULTS_PER_QUERY,
        help=f'Max URLs per query (default: {MAX_RESULTS_PER_QUERY})'
    )
    parser.add_argument(
        '--parallel',
        type=int,
        default=PARALLEL_WORKERS,
        help=f'Parallel workers (default: {PARALLEL_WORKERS})'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable debug logging'
    )

    args = parser.parse_args()

    # Setup logging
    Path('logs').mkdir(exist_ok=True)
    setup_logging(args.verbose)

    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("Production Lead Finder System Starting")
    logger.info("=" * 60)

    # Collect URLs
    candidate_urls = []

    if args.queries:
        if not Path(args.queries).exists():
            logger.error(f"Queries file not found: {args.queries}")
            sys.exit(1)

        logger.info(f"Loading queries from: {args.queries}")
        base_queries = load_queries_from_file(args.queries)

        if not base_queries:
            logger.error("No queries loaded")
            sys.exit(1)

        logger.info(f"Loaded {len(base_queries)} base queries")

        # Generate variations
        all_queries = []
        for base_query in base_queries:
            variations = generate_query_variations(base_query, include_site_types=True)
            all_queries.extend(variations)

        logger.info(f"Generated {len(all_queries)} total query variations")

        # Search using multi-engine
        logger.info("Starting multi-engine search...")
        search_engine = MultiEngineSearch()

        for query in all_queries:
            logger.info(f"Searching: {query}")
            urls = search_engine.search(query, args.limit)
            candidate_urls.extend(urls)
            logger.info(f"  Found {len(urls)} URLs")

        logger.info(f"Total candidate URLs collected: {len(candidate_urls)}")

    elif args.urls:
        if not Path(args.urls).exists():
            logger.error(f"URLs file not found: {args.urls}")
            sys.exit(1)

        logger.info(f"Loading URLs from: {args.urls}")
        candidate_urls = load_urls_from_file(args.urls)

    if not candidate_urls:
        logger.error("No URLs to process")
        sys.exit(1)

    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in candidate_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    logger.info(f"Processing {len(unique_urls)} unique URLs")

    # Process URLs
    processor = LeadProcessor(parallel_workers=args.parallel)
    leads, failed_urls = processor.process_urls(unique_urls)

    if not leads:
        logger.warning("No successful leads processed")
        sys.exit(0)

    # Deduplicate by domain
    logger.info(f"Leads before deduplication: {len(leads)}")
    leads = processor.deduplicate_leads(leads)
    logger.info(f"Leads after deduplication: {len(leads)}")

    # Sort by score (highest first)
    leads.sort(key=lambda x: x['score'], reverse=True)

    # Write output
    logger.info(f"Writing results to: {args.output}")
    OutputWriter.write_csv(leads, args.output)

    # Write failed URLs
    if failed_urls:
        failed_path = args.output.replace('.csv', '_failed_urls.txt')
        OutputWriter.write_failed_urls(failed_urls, failed_path)

    # Summary
    grade_counts = {'A': 0, 'B': 0, 'C': 0}
    for lead in leads:
        grade = lead['grade']
        grade_counts[grade] = grade_counts.get(grade, 0) + 1

    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total leads: {len(leads)}")
    logger.info(f"  Grade A (60+): {grade_counts['A']}")
    logger.info(f"  Grade B (40-59): {grade_counts['B']}")
    logger.info(f"  Grade C (<40): {grade_counts['C']}")
    logger.info(f"Failed URLs: {len(failed_urls)}")
    logger.info(f"Output: {args.output}")
    logger.info("=" * 60)
    logger.info("Processing complete!")


if __name__ == '__main__':
    main()
