"""
URL fetcher with retry logic, timeout, and user-agent handling.
"""

import logging
import time
import requests
from typing import Optional

logger = logging.getLogger(__name__)

# User agent to avoid being blocked
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

TIMEOUT = 10  # seconds
MAX_RETRIES = 2
RETRY_DELAY = 1  # seconds


def fetch_url(url: str) -> Optional[str]:
    """
    Fetch HTML content from URL with retry logic.

    Returns:
        HTML content as string, or None if failed
    """
    headers = {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
    }

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=TIMEOUT,
                allow_redirects=True
            )

            # Check if successful
            if response.status_code == 200:
                return response.text
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching {url} (attempt {attempt + 1})")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error fetching {url}: {e}")

        # Retry with delay
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    return None
