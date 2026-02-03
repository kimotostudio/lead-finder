"""
URL Liveness Checker Module

Validates URLs for HTTP reachability, follows redirects, and determines alive/dead status.

Liveness Rules:
- ALIVE: HTTP status in {200, 201, 202, 203, 204, 206, 301, 302, 307, 308, 401, 403}
- DEAD: 404, 410, DNS failure, connection error, timeout, repeated 5xx

Features:
- Concurrent checking with ThreadPoolExecutor
- Redirect following (up to 5 hops)
- Exponential backoff retries
- Rate limiting with jitter
"""
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests.exceptions import (
    ConnectionError,
    Timeout,
    TooManyRedirects,
    RequestException,
)

logger = logging.getLogger(__name__)

# HTTP status codes considered "alive"
ALIVE_STATUS_CODES = {200, 201, 202, 203, 204, 206, 301, 302, 307, 308, 401, 403}

# HTTP status codes considered "dead"
DEAD_STATUS_CODES = {404, 410}

# Default configuration
DEFAULT_CONFIG = {
    'connect_timeout': 5,      # seconds
    'read_timeout': 10,        # seconds
    'max_retries': 2,
    'max_redirects': 5,
    'max_workers': 10,
    'jitter_min': 0.1,         # seconds
    'jitter_max': 0.3,         # seconds
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}


class LivenessChecker:
    """Checks URL liveness with concurrent requests and retry logic."""

    def __init__(
        self,
        connect_timeout: int = DEFAULT_CONFIG['connect_timeout'],
        read_timeout: int = DEFAULT_CONFIG['read_timeout'],
        max_retries: int = DEFAULT_CONFIG['max_retries'],
        max_redirects: int = DEFAULT_CONFIG['max_redirects'],
        max_workers: int = DEFAULT_CONFIG['max_workers'],
        jitter_min: float = DEFAULT_CONFIG['jitter_min'],
        jitter_max: float = DEFAULT_CONFIG['jitter_max'],
        user_agent: str = DEFAULT_CONFIG['user_agent'],
    ):
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.max_retries = max_retries
        self.max_redirects = max_redirects
        self.max_workers = max_workers
        self.jitter_min = jitter_min
        self.jitter_max = jitter_max
        self.user_agent = user_agent

        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        # Disable SSL verification warnings for speed (many small sites have issues)
        self.session.verify = False

    def _add_jitter(self):
        """Add random delay between requests."""
        time.sleep(random.uniform(self.jitter_min, self.jitter_max))

    def check_url(self, url: str) -> Dict:
        """
        Check a single URL for liveness.

        Args:
            url: URL to check

        Returns:
            Dict with keys: url, http_status, final_url, is_alive, checked_at_iso, error
        """
        result = {
            'url': url,
            'http_status': None,
            'final_url': url,
            'is_alive': False,
            'checked_at_iso': datetime.now().isoformat(timespec='seconds'),
            'error': None,
        }

        if not url or not url.strip():
            result['error'] = 'Empty URL'
            return result

        # Ensure URL has protocol
        check_url = url.strip()
        if not check_url.startswith(('http://', 'https://')):
            check_url = 'https://' + check_url

        backoff = 1  # Initial backoff in seconds

        for attempt in range(self.max_retries + 1):
            try:
                self._add_jitter()

                # Use GET with stream=True to avoid downloading full content
                # Also send Range header to minimize data transfer
                response = self.session.get(
                    check_url,
                    timeout=(self.connect_timeout, self.read_timeout),
                    allow_redirects=True,
                    stream=True,
                    headers={'Range': 'bytes=0-0'},
                )

                # Get final URL after redirects
                result['final_url'] = response.url
                result['http_status'] = response.status_code

                # Determine if alive
                if response.status_code in ALIVE_STATUS_CODES:
                    result['is_alive'] = True
                elif response.status_code in DEAD_STATUS_CODES:
                    result['is_alive'] = False
                    result['error'] = f'HTTP {response.status_code}'
                elif 500 <= response.status_code < 600:
                    # 5xx errors - retry
                    if attempt < self.max_retries:
                        logger.debug(f"Retry {attempt + 1} for {url}: HTTP {response.status_code}")
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                    result['is_alive'] = False
                    result['error'] = f'Server error: HTTP {response.status_code}'
                else:
                    # Other status codes - treat as alive if site responds
                    result['is_alive'] = True

                # Close the response to free connection
                response.close()
                break

            except TooManyRedirects:
                result['error'] = 'Too many redirects'
                result['is_alive'] = False
                break

            except Timeout:
                if attempt < self.max_retries:
                    logger.debug(f"Timeout retry {attempt + 1} for {url}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                result['error'] = 'Timeout'
                result['is_alive'] = False

            except ConnectionError as e:
                error_str = str(e).lower()
                if 'name or service not known' in error_str or 'nodename nor servname' in error_str:
                    result['error'] = 'DNS failure'
                    result['is_alive'] = False
                    break  # DNS failures don't benefit from retry
                if attempt < self.max_retries:
                    logger.debug(f"Connection retry {attempt + 1} for {url}: {e}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                result['error'] = f'Connection error: {str(e)[:100]}'
                result['is_alive'] = False

            except RequestException as e:
                if attempt < self.max_retries:
                    logger.debug(f"Request retry {attempt + 1} for {url}: {e}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                result['error'] = f'Request error: {str(e)[:100]}'
                result['is_alive'] = False

            except Exception as e:
                result['error'] = f'Unexpected error: {str(e)[:100]}'
                result['is_alive'] = False
                break

        return result

    def check_urls(self, urls: List[str], progress_callback=None) -> Dict[str, Dict]:
        """
        Check multiple URLs concurrently.

        Args:
            urls: List of URLs to check
            progress_callback: Optional callback(current, total, url, result)

        Returns:
            Dict mapping URL to result dict
        """
        results = {}
        total = len(urls)

        logger.info(f"Checking liveness for {total} URLs with {self.max_workers} workers")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {
                executor.submit(self.check_url, url): url
                for url in urls
            }

            for i, future in enumerate(as_completed(future_to_url), 1):
                url = future_to_url[future]
                try:
                    result = future.result()
                    results[url] = result

                    if not result['is_alive']:
                        logger.debug(f"Dead URL: {url} - {result.get('error', 'Unknown')}")

                    if progress_callback:
                        progress_callback(i, total, url, result)

                except Exception as e:
                    logger.error(f"Error checking {url}: {e}")
                    results[url] = {
                        'url': url,
                        'http_status': None,
                        'final_url': url,
                        'is_alive': False,
                        'checked_at_iso': datetime.now().isoformat(timespec='seconds'),
                        'error': str(e),
                    }

        alive_count = sum(1 for r in results.values() if r['is_alive'])
        logger.info(f"Liveness check complete: {alive_count}/{total} alive")

        return results

    def close(self):
        """Close the session."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def check_leads_liveness(
    leads: List[Dict],
    keep_dead: bool = False,
    max_workers: int = DEFAULT_CONFIG['max_workers'],
    progress_callback=None,
) -> List[Dict]:
    """
    Check liveness for a list of lead dicts and update them with liveness fields.

    Args:
        leads: List of lead dicts (must have 'url' field)
        keep_dead: If True, keep dead leads with is_alive=False; if False, filter them out
        max_workers: Number of concurrent workers
        progress_callback: Optional callback(current, total, url, result)

    Returns:
        List of leads with liveness fields added (filtered if keep_dead=False)
    """
    if not leads:
        return []

    # Extract unique URLs
    url_to_leads = {}
    for lead in leads:
        url = lead.get('url', '').strip()
        if url:
            if url not in url_to_leads:
                url_to_leads[url] = []
            url_to_leads[url].append(lead)

    unique_urls = list(url_to_leads.keys())
    logger.info(f"Checking {len(unique_urls)} unique URLs from {len(leads)} leads")

    # Check liveness
    with LivenessChecker(max_workers=max_workers) as checker:
        results = checker.check_urls(unique_urls, progress_callback)

    # Update leads with liveness data
    updated_leads = []
    for lead in leads:
        url = lead.get('url', '').strip()
        if url and url in results:
            result = results[url]
            lead_copy = lead.copy()
            lead_copy['http_status'] = result['http_status']
            lead_copy['final_url'] = result['final_url']
            lead_copy['is_alive'] = result['is_alive']
            lead_copy['checked_at_iso'] = result['checked_at_iso']

            if keep_dead or result['is_alive']:
                updated_leads.append(lead_copy)
        elif not url:
            # URL missing - treat as dead
            lead_copy = lead.copy()
            lead_copy['http_status'] = None
            lead_copy['final_url'] = ''
            lead_copy['is_alive'] = False
            lead_copy['checked_at_iso'] = datetime.now().isoformat(timespec='seconds')
            if keep_dead:
                updated_leads.append(lead_copy)

    alive_count = sum(1 for l in updated_leads if l.get('is_alive', False))
    logger.info(f"Result: {alive_count} alive, {len(updated_leads) - alive_count} dead"
                + (" (keeping dead)" if keep_dead else " (filtered)"))

    return updated_leads


def extract_domain(url: str) -> str:
    """
    Extract normalized domain from URL for deduplication.

    Rules:
    - lowercase
    - strip protocol
    - strip leading www.
    - ignore path/query (domain only)

    Args:
        url: URL string

    Returns:
        Normalized domain string
    """
    if not url:
        return ''

    try:
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        # Remove www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]

        return domain

    except Exception:
        return ''


def dedupe_by_domain(
    leads: List[Dict],
    use_final_url: bool = True,
) -> List[Dict]:
    """
    Deduplicate leads by normalized domain, keeping highest score.

    Args:
        leads: List of lead dicts
        use_final_url: If True, use final_url for domain extraction when available

    Returns:
        Deduplicated list of leads
    """
    domain_to_lead = {}

    for lead in leads:
        # Get URL for domain extraction
        if use_final_url and lead.get('final_url'):
            url = lead['final_url']
        else:
            url = lead.get('url', '')

        domain = extract_domain(url)
        if not domain:
            continue

        score = lead.get('score', 0)
        if isinstance(score, str):
            try:
                score = int(score)
            except ValueError:
                score = 0

        if domain not in domain_to_lead:
            domain_to_lead[domain] = lead
        else:
            existing_score = domain_to_lead[domain].get('score', 0)
            if isinstance(existing_score, str):
                try:
                    existing_score = int(existing_score)
                except ValueError:
                    existing_score = 0
            if score > existing_score:
                domain_to_lead[domain] = lead

    result = list(domain_to_lead.values())
    logger.info(f"Domain dedup: {len(leads)} -> {len(result)} leads")

    return result
