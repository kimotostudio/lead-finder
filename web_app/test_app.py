#!/usr/bin/env python3
"""
Simple test script for web app
"""
import requests
import time
import json

BASE_URL = 'http://localhost:5000'

def test_homepage():
    """Test homepage loads"""
    print("Testing homepage...")
    response = requests.get(f'{BASE_URL}/')
    assert response.status_code == 200
    assert 'Lead Finder' in response.text
    print("  OK: Homepage loads successfully")

def test_api_test():
    """Test /api/test endpoint"""
    print("Testing /api/test endpoint...")
    response = requests.get(f'{BASE_URL}/api/test')
    data = response.json()
    assert data['status'] == 'ok'
    assert '東京' in data['cities']
    assert 'カウンセリング' in data['business_types']
    print("  OK: Test endpoint works")

def test_search_minimal():
    """Test minimal search"""
    print("Testing minimal search (1 city, 1 business type, limit 3)...")

    # Start search
    search_request = {
        'region': '東京',
        'cities': ['新宿区'],
        'business_types': ['カウンセリング'],
        'limit': 3,
        'min_score': 0
    }

    print(f"  Sending search request: {json.dumps(search_request, ensure_ascii=False)}")
    response = requests.post(
        f'{BASE_URL}/api/search',
        json=search_request
    )

    assert response.status_code == 200
    data = response.json()
    assert data['status'] == 'started'
    print("  OK: Search started")

    # Poll for progress
    max_wait = 120  # 2 minutes
    start_time = time.time()

    while time.time() - start_time < max_wait:
        progress = requests.get(f'{BASE_URL}/api/progress').json()
        status = progress['status']
        current = progress.get('current', 0)
        total = progress.get('total', 0)

        print(f"  Status: {status}, Progress: {current}/{total}, Message: {progress.get('message', '')}")

        if status == 'completed':
            results = progress.get('results', [])
            print(f"  OK: Search completed with {len(results)} results")

            if results:
                print(f"  Sample result:")
                print(f"    Name: {results[0]['name']}")
                print(f"    URL: {results[0]['url']}")
                print(f"    Score: {results[0]['score']}")
                print(f"    Grade: {results[0]['grade']}")

            return True

        elif status == 'error':
            print(f"  ERROR: {progress.get('message', 'Unknown error')}")
            return False

        time.sleep(2)

    print("  ERROR: Timeout waiting for search to complete")
    return False

def main():
    print("=" * 60)
    print("Lead Finder Web App - Integration Test")
    print("=" * 60)
    print()

    try:
        test_homepage()
        print()
        test_api_test()
        print()
        test_search_minimal()
        print()
        print("=" * 60)
        print("All tests passed! ✓")
        print("=" * 60)

    except AssertionError as e:
        print(f"\nTest failed: {e}")
    except requests.exceptions.ConnectionError:
        print("\nERROR: Cannot connect to server. Is it running?")
        print("Run: python app.py")
    except Exception as e:
        print(f"\nUnexpected error: {e}")

if __name__ == '__main__':
    main()
