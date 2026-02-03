import sys
from pathlib import Path
import requests
import json

# add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.processor import LeadProcessor

API_TEST = 'http://127.0.0.1:5000/api/test'
API_PROGRESS = 'http://127.0.0.1:5000/api/progress'


def get_urls_from_api():
    try:
        r = requests.get(API_PROGRESS, timeout=5)
        r.raise_for_status()
        data = r.json()
        urls = [item.get('url') for item in data.get('results', []) if item.get('url')]
        return urls
    except Exception as e:
        print('Failed to fetch /api/progress:', e)
        return []


def main():
    urls = []
    if len(sys.argv) > 1:
        urls = sys.argv[1:]
    else:
        urls = get_urls_from_api()

    if not urls:
        print('No URLs to test. Provide URLs as arguments or ensure web app has recent results.')
        sys.exit(1)

    print('Testing URLs:', urls)

    processor = LeadProcessor(parallel_workers=3)
    final_leads, failed, filtered = processor.process_pipeline(urls)

    print('\nResults:')
    for lead in final_leads:
        print('---')
        print('shop_name:', lead.get('shop_name'))
        print('url:', lead.get('url'))
        print('score:', lead.get('score'))
        print('grade:', lead.get('grade'))
        print('weakness_score:', lead.get('weakness_score', 'N/A'))
        print('reasons:', lead.get('reasons'))
        print('site_type:', lead.get('site_type'))
        print('business_type:', lead.get('business_type'))
        print()

    print('Failed urls:', failed)


if __name__ == '__main__':
    main()
