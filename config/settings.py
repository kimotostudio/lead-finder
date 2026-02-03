"""
Configuration settings for the lead finder system.
All values can be overridden via environment variables.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# API Keys (optional)
BING_API_KEY = os.getenv('BING_API_KEY', '')
BRAVE_API_KEY = os.getenv('BRAVE_API_KEY', '')

# Request settings
TIMEOUT = int(os.getenv('TIMEOUT', '15'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RATE_LIMIT_DELAY = float(os.getenv('RATE_LIMIT_DELAY', '2.0'))

# Processing settings
PARALLEL_WORKERS = int(os.getenv('PARALLEL_WORKERS', '10'))
MAX_RESULTS_PER_QUERY = int(os.getenv('MAX_RESULTS_PER_QUERY', '20'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# Scoring thresholds
GRADE_A_THRESHOLD = 60
GRADE_B_THRESHOLD = 40

# Max redirects
MAX_REDIRECTS = 3
