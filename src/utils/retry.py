"""
Retry utilities with exponential backoff.
"""
import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def exponential_backoff_retry(max_retries=3, base_delay=1.0, max_delay=10.0):
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = min(delay, max_delay)
                        logger.debug(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        delay *= 2  # Exponential backoff
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}")

            raise last_exception

        return wrapper
    return decorator
