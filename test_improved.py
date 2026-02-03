"""
Test script to validate improved lead finder with specific area.
Tests aggregator filtering and proper shop name extraction.
"""
import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.engines.multi_engine import MultiEngineSearch
from src.processor import LeadProcessor
from src.output_writer import OutputWriter
from src.utils.query_generator import generate_query_variations

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)

# Suppress noisy libraries
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('primp').setLevel(logging.WARNING)
logging.getLogger('rquest').setLevel(logging.WARNING)
logging.getLogger('cookie_store').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def test_small_area():
    """Test with a specific small area query."""

    # Test query - specific area for focused results
    base_query = "川崎市 個人 エステサロン"

    logger.info("=" * 60)
    logger.info("IMPROVED LEAD FINDER TEST")
    logger.info("=" * 60)
    logger.info(f"Base query: {base_query}")

    # Generate query variations
    queries = generate_query_variations(base_query, include_site_types=True)
    logger.info(f"Generated {len(queries)} query variations")

    # Limit to first 5 variations for quick test
    queries = queries[:5]
    logger.info(f"Testing with {len(queries)} variations:")
    for i, q in enumerate(queries, 1):
        logger.info(f"  {i}. {q}")

    # Search
    logger.info("\n" + "=" * 60)
    logger.info("SEARCHING...")
    logger.info("=" * 60)

    search_engine = MultiEngineSearch()
    all_urls = []

    for query in queries:
        logger.info(f"\nSearching: {query}")
        urls = search_engine.search(query, max_results_per_engine=5)
        all_urls.extend(urls)
        logger.info(f"  Found {len(urls)} URLs")

    # Remove duplicates
    unique_urls = list(dict.fromkeys(all_urls))
    logger.info(f"\nTotal unique URLs: {len(unique_urls)}")

    if not unique_urls:
        logger.error("No URLs found! Try different query.")
        return

    # Process URLs
    logger.info("\n" + "=" * 60)
    logger.info("PROCESSING URLs (with aggregator filtering)...")
    logger.info("=" * 60)

    processor = LeadProcessor(parallel_workers=3)
    leads, failed_urls = processor.process_urls(unique_urls)

    logger.info("\n" + "=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)
    logger.info(f"Total URLs searched: {len(unique_urls)}")
    logger.info(f"Individual business leads found: {len(leads)}")
    logger.info(f"Aggregators/failed filtered out: {len(failed_urls)}")

    if not leads:
        logger.warning("\nNo individual business leads found!")
        logger.warning("This could mean:")
        logger.warning("1. All results were aggregator sites (filtered out)")
        logger.warning("2. All sites failed to crawl")
        logger.warning("3. Try a different query/area")
        return

    # Deduplicate
    leads = processor.deduplicate_leads(leads)

    # Sort by score
    leads.sort(key=lambda x: x['score'], reverse=True)

    # Display sample results
    logger.info("\n" + "=" * 60)
    logger.info("SAMPLE LEADS (Top 5)")
    logger.info("=" * 60)

    for i, lead in enumerate(leads[:5], 1):
        logger.info(f"\n{i}. {lead['shop_name']}")
        logger.info(f"   Type: {lead['business_type']}")
        logger.info(f"   Score: {lead['score']} ({lead['grade']})")
        logger.info(f"   Site: {lead['site_type']}")
        logger.info(f"   URL: {lead['url']}")
        if lead['owner_name']:
            logger.info(f"   Owner: {lead['owner_name']}")
        if lead['phone']:
            logger.info(f"   Phone: {lead['phone']}")
        if lead['address']:
            logger.info(f"   Address: {lead['address'][:50]}...")

    # Save to CSV
    output_path = "output/test_improved_leads.csv"
    OutputWriter.write_csv(leads, output_path)

    logger.info("\n" + "=" * 60)
    logger.info("COMPLETE!")
    logger.info("=" * 60)
    logger.info(f"Results saved to: {output_path}")
    logger.info(f"Total individual business leads: {len(leads)}")

    # Grade breakdown
    grade_counts = {'A': 0, 'B': 0, 'C': 0}
    for lead in leads:
        grade = lead['grade']
        grade_counts[grade] = grade_counts.get(grade, 0) + 1

    logger.info(f"  Grade A (60+): {grade_counts['A']}")
    logger.info(f"  Grade B (40-59): {grade_counts['B']}")
    logger.info(f"  Grade C (<40): {grade_counts['C']}")

    # Business type breakdown
    type_counts = {}
    for lead in leads:
        btype = lead['business_type']
        type_counts[btype] = type_counts.get(btype, 0) + 1

    logger.info("\nBusiness types found:")
    for btype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {btype}: {count}")


if __name__ == '__main__':
    try:
        test_small_area()
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
    except Exception as e:
        logger.error(f"\nTest failed with error: {e}", exc_info=True)
