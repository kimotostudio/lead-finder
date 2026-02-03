#!/usr/bin/env python3
"""
Advanced search script using multi-layer query strategy.
Implements deep discovery for counseling/therapy/wellness businesses.
"""
import argparse
import logging
from pathlib import Path
from config.advanced_queries import generate_queries, generate_test_queries, TARGET_REGIONS
from src.engines.multi_engine import MultiEngineSearch
from src.processor import LeadProcessor
from src.output_writer import OutputWriter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Advanced multi-layer search for individual businesses'
    )
    parser.add_argument(
        '--region',
        required=True,
        choices=list(TARGET_REGIONS.keys()),
        help='Target region (神奈川, 埼玉, 東京)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run test mode with limited queries'
    )
    parser.add_argument(
        '--cities-limit',
        type=int,
        default=3,
        help='Number of cities to include in test mode (default: 3)'
    )
    parser.add_argument(
        '--queries-per-city',
        type=int,
        default=5,
        help='Queries per city in test mode (default: 5)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='URLs per query (default: 10)'
    )
    parser.add_argument(
        '--parallel',
        type=int,
        default=5,
        help='Parallel workers (default: 5)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output CSV path (default: output/advanced_{region}.csv)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Verbose logging'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Generate queries
    logger.info(f"Generating queries for region: {args.region}")

    if args.test:
        queries = generate_test_queries(
            args.region,
            cities_limit=args.cities_limit,
            queries_per_city=args.queries_per_city
        )
        logger.info(f"TEST MODE: Generated {len(queries)} test queries")
    else:
        queries = generate_queries(args.region)
        logger.info(f"FULL MODE: Generated {len(queries)} queries across all layers")

    logger.info(f"Sample queries:")
    for q in queries[:5]:
        logger.info(f"  - {q}")

    # Setup output path
    if args.output:
        output_path = args.output
    else:
        mode = 'test' if args.test else 'full'
        output_path = f"output/advanced_{args.region}_{mode}.csv"

    logger.info(f"Output will be saved to: {output_path}")

    # Search URLs
    logger.info(f"Starting search with {args.limit} URLs per query...")
    searcher = MultiEngineSearch()
    all_urls = set()

    for i, query in enumerate(queries, 1):
        logger.info(f"[{i}/{len(queries)}] Searching: {query}")
        try:
            urls = searcher.search(query, max_results_per_engine=args.limit)
            logger.info(f"  Found {len(urls)} URLs")
            all_urls.update(urls)
        except Exception as e:
            logger.error(f"  Error searching '{query}': {e}")
            continue

    logger.info(f"Total unique URLs collected: {len(all_urls)}")

    if len(all_urls) == 0:
        logger.warning("No URLs found. Exiting.")
        return

    # Process URLs
    logger.info(f"Processing {len(all_urls)} URLs with {args.parallel} workers...")
    processor = LeadProcessor(parallel_workers=args.parallel)
    leads, failed_urls = processor.process_urls(list(all_urls))

    logger.info(f"Leads before deduplication: {len(leads)}")

    # Deduplicate
    logger.info("Deduplicating leads...")
    unique_leads = processor.deduplicate_leads(leads)
    logger.info(f"Leads after deduplication: {len(unique_leads)}")

    # Sort by score descending
    unique_leads.sort(key=lambda x: x['score'], reverse=True)

    # Write output
    logger.info(f"Writing results to: {output_path}")
    OutputWriter.write_csv(unique_leads, output_path)

    # Write failed URLs
    failed_path = output_path.replace('.csv', '_failed_urls.txt')
    OutputWriter.write_failed_urls(failed_urls, failed_path)

    # Summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Region: {args.region}")
    logger.info(f"Mode: {'TEST' if args.test else 'FULL'}")
    logger.info(f"Queries executed: {len(queries)}")
    logger.info(f"Total URLs found: {len(all_urls)}")
    logger.info(f"Total leads: {len(unique_leads)}")

    # Grade breakdown
    grade_counts = {}
    for lead in unique_leads:
        grade = lead['grade']
        grade_counts[grade] = grade_counts.get(grade, 0) + 1

    logger.info(f"  Grade A (60+): {grade_counts.get('A', 0)}")
    logger.info(f"  Grade B (40-59): {grade_counts.get('B', 0)}")
    logger.info(f"  Grade C (<40): {grade_counts.get('C', 0)}")

    # Business type breakdown
    type_counts = {}
    for lead in unique_leads:
        btype = lead.get('business_type', '不明')
        type_counts[btype] = type_counts.get(btype, 0) + 1

    logger.info(f"Business types found:")
    for btype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {btype}: {count}")

    logger.info(f"Failed URLs: {len(failed_urls)}")
    logger.info(f"Output: {output_path}")
    logger.info("=" * 60)
    logger.info("Processing complete!")


if __name__ == '__main__':
    main()
