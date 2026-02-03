"""
Quick test with targeted queries for individual businesses.
"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.engines.multi_engine import MultiEngineSearch
from src.processor import LeadProcessor
from src.output_writer import OutputWriter

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.ERROR)
logging.getLogger('primp').setLevel(logging.ERROR)
logging.getLogger('rquest').setLevel(logging.ERROR)
logging.getLogger('cookie_store').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

def main():
    # Target peraichi and ameblo sites explicitly
    queries = [
        "渋谷 ヨガ site:peraichi.com",
        "川崎 エステ site:ameblo.jp",
        "新宿 整体 site:wixsite.com",
    ]

    logger.info("="*60)
    logger.info("QUICK TEST - Individual Business Sites")
    logger.info("="*60)

    search_engine = MultiEngineSearch()
    all_urls = []

    for query in queries:
        logger.info(f"\nSearching: {query}")
        urls = search_engine.search(query, max_results_per_engine=10)
        all_urls.extend(urls)
        logger.info(f"Found: {len(urls)} URLs")

    unique_urls = list(dict.fromkeys(all_urls))
    logger.info(f"\nTotal unique URLs: {len(unique_urls)}\n")

    if not unique_urls:
        logger.error("No URLs found!")
        return

    # Process
    logger.info("="*60)
    logger.info("PROCESSING (with improved extraction)...")
    logger.info("="*60)

    processor = LeadProcessor(parallel_workers=5)
    leads, failed = processor.process_urls(unique_urls)

    logger.info("\n" + "="*60)
    logger.info(f"RESULTS: {len(leads)} individual businesses found")
    logger.info("="*60)

    if leads:
        # Deduplicate and sort
        leads = processor.deduplicate_leads(leads)
        leads.sort(key=lambda x: x['score'], reverse=True)

        # Show top 10
        logger.info("\nTOP LEADS:")
        for i, lead in enumerate(leads[:10], 1):
            logger.info(f"\n{i}. {lead['shop_name']}")
            logger.info(f"   Type: {lead['business_type']} | Score: {lead['score']} ({lead['grade']})")
            logger.info(f"   Platform: {lead['site_type']}")
            if lead['phone']:
                logger.info(f"   Phone: {lead['phone']}")
            if lead['owner_name']:
                logger.info(f"   Owner: {lead['owner_name']}")
            logger.info(f"   URL: {lead['url']}")

        # Save
        OutputWriter.write_csv(leads, "output/quick_test.csv")
        logger.info(f"\n✓ Saved {len(leads)} leads to output/quick_test.csv")

        # Stats
        grade_counts = {}
        type_counts = {}
        for lead in leads:
            grade_counts[lead['grade']] = grade_counts.get(lead['grade'], 0) + 1
            type_counts[lead['business_type']] = type_counts.get(lead['business_type'], 0) + 1

        logger.info(f"\nGrades: A={grade_counts.get('A',0)}, B={grade_counts.get('B',0)}, C={grade_counts.get('C',0)}")
        logger.info(f"Types: {dict(sorted(type_counts.items(), key=lambda x: x[1], reverse=True))}")

if __name__ == '__main__':
    main()
