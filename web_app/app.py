#!/usr/bin/env python3
"""
Lead Finder Web Application
Flask-based web interface for advanced_search.py

Enhanced with:
- Improved query strategy targeting actionable sites
- Hard URL filtering to exclude portals/blogs
- Mini precheck for contact/booking signals
- Spiritual business type enhancements
- Source tracking and observability
- AI verification using GPT-4o-mini
"""
import os
import sys

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
# Wrap sys.stderr to prevent OSError from flush/write in background threads
# On Windows, background Flask threads can have invalid stderr handles
_orig_stderr = sys.stderr
class _SafeErr:
    def write(self, *args, **kwargs):
        try:
            return _orig_stderr.write(*args, **kwargs)
        except (OSError, ValueError, AttributeError):
            return None

    def flush(self, *args, **kwargs):
        # Completely disable flush to avoid OSError [Errno 22] on Windows
        # This is safe because we're only disabling stderr flush, not stdout
        return None

    def fileno(self):
        try:
            return _orig_stderr.fileno()
        except Exception:
            return -1

    def isatty(self):
        return False

    def __getattr__(self, name):
        # Forward any other attribute access to original stderr, with safety
        try:
            return getattr(_orig_stderr, name)
        except Exception:
            return None

sys.stderr = _SafeErr()
import re
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, request, jsonify, send_file, session
from flask_cors import CORS
import threading
import requests

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Early workaround: disable tqdm status_printer and tqdm.tqdm
# to avoid OSError ([Errno 22] Invalid argument) when background
# threads or worker pools attempt to write/flush to stderr on Windows.
try:
    import tqdm.std as _tqdm_std

    def _safe_status_printer(fp):
        class _DummyPrinter:
            def write(self, *args, **kwargs):
                return None

            def flush(self, *args, **kwargs):
                return None

        return _DummyPrinter()

    _tqdm_std.status_printer = _safe_status_printer
except Exception:
    pass

try:
    import tqdm as _tqdm_module

    class _DummyTqdm:
        def __init__(self, *args, **kwargs):
            self.total = kwargs.get('total', 0)

        def update(self, *args, **kwargs):
            return None

        def close(self, *args, **kwargs):
            return None

        def __iter__(self):
            return iter(())

    _tqdm_module.tqdm = _DummyTqdm
    try:
        import tqdm.std as _tq_std
        _tq_std.tqdm = _DummyTqdm
    except Exception:
        pass
except Exception:
    pass

from config.advanced_queries import TARGET_REGIONS
from config.cities_data import REGIONS, CITIES_BY_PREFECTURE, get_prefectures_by_region, get_cities_by_prefecture
from src.engines.multi_engine import MultiEngineSearch
from src.processor import LeadProcessor
from src.output_writer import OutputWriter
from src.japanese_detector import classify_url_japanese
from src.content_analyzer import has_japanese_content
# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.jinja_env.auto_reload = True
CORS(app)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
try:
    template_paths = getattr(app.jinja_loader, 'searchpath', [])
    logger.info(f"Template search paths: {template_paths}")
    if template_paths:
        index_path = Path(template_paths[0]) / 'index.html'
        if index_path.exists():
            content = index_path.read_text(encoding='utf-8', errors='replace')
            logger.info(
                "Template index.html flags: checkbox=%s select=%s",
                'solo-class-solo' in content,
                'soloClassSelect' in content,
            )
except Exception as exc:
    logger.warning("Template path check failed: %s", exc)

# ============================================================
# EXCLUDED DOMAINS - Block at query level and URL level
# ============================================================
EXCLUDED_DOMAINS = [
    # Blog platforms (cannot contact directly)
    'ameblo.jp',
    'amebaownd.com',
    'ameba.jp',
    's.ameblo.jp',
    'profile.ameba.jp',
    'note.com',
    'note.mu',
    'hatenablog.com',
    'hatenablog.jp',
    'hatena.ne.jp',
    'livedoor.blog',
    'livedoor.jp',
    'blogspot.com',
    'fc2.com',
    'fc2blog.net',
    'blog.fc2.com',
    'muragon.com',
    'goo.ne.jp',
    'seesaa.net',

    # Portal / aggregator sites
    'hotpepper.jp',
    'hotpepperbeauty.jp',
    'beauty.hotpepper.jp',
    'beauty.rakuten.co.jp',
    'tabelog.com',
    'epark.jp',
    'ekiten.jp',
    'rakuten.co.jp',
    'rakuten.ne.jp',
    'gnavi.co.jp',
    'gurunavi.com',
    'jalan.net',
    'minkou.jp',
    'caloo.jp',
    'benri.com',
    'itp.ne.jp',
    'townpage.goo.ne.jp',
    'navitokyo.com',
    'loco.yahoo.co.jp',
    'yelp.co.jp',
    'zozo.jp',
    'amazon.co.jp',
    'yahoo.co.jp',

    # SNS platforms
    'instagram.com',
    'twitter.com',
    'x.com',
    'facebook.com',
    'tiktok.com',
    'youtube.com',
    'line.me',
    'linkedin.com',
    'pinterest.com',

    # Link aggregators
    'lit.link',
    'litlink.jp',
    'linktr.ee',
    'linktree.com',

    # Maps
    'goo.gl',
    'maps.google.com',
    'google.com/maps',

    # Job sites
    'indeed.com',
    'indeed.jp',
    'rikunabi.com',
    'mynavi.jp',
    'doda.jp',
    'en-japan.com',
    'careerjet.jp',
    'stanby.co.jp',

    # Other directories
    'byoinnavi.jp',
    'kenkou-job.com',
    'b-spot.tv',
    'wikipedia.org',
]

# Spiritual business types that get enhanced query variants
SPIRITUAL_BUSINESS_TYPES = {
    'ヒーリング', 'スピリチュアル', 'エネルギーワーク', 'レイキ', 'チャネリング',
    '霊視', 'オーラ鑑定', '波動調整', '浄化', '遠隔ヒーリング',
    'タロット占い', '占星術', '手相占い', '数秘術', '風水', 'パワーストーン',
}

# Business types organized by category
BUSINESS_TYPE_CATEGORIES = [
    {
        'id': 'mental',
        'name': 'メンタル・カウンセリング',
        'icon': 'bi-chat-heart',
        'types': [
            'カウンセリング', '心理カウンセリング', 'コーチング', 'セラピー',
            'ライフコーチング', 'キャリアカウンセリング', '催眠療法',
            'NLP', 'マインドフルネス', '瞑想教室',
        ],
    },
    {
        'id': 'spiritual',
        'name': 'スピリチュアル・ヒーリング',
        'icon': 'bi-brightness-high',
        'types': [
            'ヒーリング', 'スピリチュアル', 'エネルギーワーク', 'レイキ',
            'チャネリング', '霊視', 'オーラ鑑定', '波動調整', '浄化',
            '遠隔ヒーリング', 'タロット占い', '占星術', '手相占い',
            '数秘術', '風水', 'パワーストーン',
        ],
    },
    {
        'id': 'wellness',
        'name': 'ウェルネス・ボディケア',
        'icon': 'bi-heart-pulse',
        'types': [
            '整体', '鍼灸', 'マッサージ', 'カイロプラクティック',
            'リフレクソロジー', '骨盤矯正', '小顔矯正', 'リンパマッサージ',
            'アロマセラピー', 'タイ古式マッサージ', '足つぼ', '指圧',
        ],
    },
    {
        'id': 'fitness',
        'name': 'フィットネス・運動',
        'icon': 'bi-activity',
        'types': [
            'ヨガ', 'ピラティス', 'パーソナルトレーニング', 'ストレッチ',
            '加圧トレーニング', 'ダンス教室', '太極拳', '気功',
            'フィットネス', 'パーソナルジム',
        ],
    },
    {
        'id': 'beauty',
        'name': '美容・エステ',
        'icon': 'bi-stars',
        'types': [
            'エステ', 'フェイシャルエステ', '痩身エステ', '脱毛サロン',
            'ネイルサロン', 'まつ毛エクステ', '眉毛サロン', 'ヘッドスパ',
            '美容鍼', 'よもぎ蒸し',
        ],
    },
    {
        'id': 'education',
        'name': '教育・スクール',
        'icon': 'bi-book',
        'types': [
            '英会話教室', 'ピアノ教室', '書道教室', '料理教室',
            'フラワーアレンジメント', '着付け教室', '茶道教室',
            'プログラミング教室', '音楽教室', '学習塾',
        ],
    },
    {
        'id': 'lifestyle',
        'name': 'ライフスタイル・相談',
        'icon': 'bi-house-heart',
        'types': [
            'ファイナンシャルプランナー', '保険相談', '不動産相談',
            'インテリアコーディネーター', '片付けコンサルタント',
            '婚活アドバイザー', '終活カウンセラー', 'ペットシッター',
            'ドッグトレーナー', '写真スタジオ',
        ],
    },
    {
        'id': 'creative',
        'name': 'クリエイティブ・デザイン',
        'icon': 'bi-palette',
        'types': [
            'Webデザイン', 'グラフィックデザイン', '動画制作',
            'カメラマン', 'イラストレーター', 'ハンドメイド教室',
            '陶芸教室', '絵画教室',
        ],
    },
]

# Derive flat list for backward compatibility
BUSINESS_TYPES = []
for _cat in BUSINESS_TYPE_CATEGORIES:
    BUSINESS_TYPES.extend(_cat['types'])

# ============================================================
# CONFIGURATION KNOBS (via environment variables)
# ============================================================
MIN_URLS_PER_PAIR = int(os.environ.get('MIN_URLS_PER_PAIR', '3'))
MAX_URLS_TO_PROCESS = int(os.environ.get('MAX_URLS_TO_PROCESS', '1500'))
MAX_QUERIES_TOTAL = int(os.environ.get('MAX_QUERIES_TOTAL', '800'))
LEAD_PROCESS_WORKERS = int(os.environ.get('LEAD_PROCESS_WORKERS', '12'))
AI_RELEVANCE_TOP_N = int(os.environ.get('AI_RELEVANCE_TOP_N', '30'))
AI_RELEVANCE_MIN_CONFIDENCE = int(os.environ.get('AI_RELEVANCE_MIN_CONFIDENCE', '6'))
MAX_URLS_PER_DOMAIN = int(os.environ.get('MAX_URLS_PER_DOMAIN', '15'))
PRECHECK_UNKNOWN_KEEP_RATIO = float(os.environ.get('PRECHECK_UNKNOWN_KEEP_RATIO', '0.9'))
PRECHECK_THIN_PENALTY = int(os.environ.get('PRECHECK_THIN_PENALTY', '1'))
FOREIGN_FILTER_MODE = os.environ.get('FOREIGN_FILTER_MODE', 'strict').strip().lower()
PRECHECK_NEGATIVE_KEEP_RATIO = float(os.environ.get('PRECHECK_NEGATIVE_KEEP_RATIO', '0.5'))
PRECHECK_FALLBACK_MIN_RATIO = float(os.environ.get('PRECHECK_FALLBACK_MIN_RATIO', '0.4'))
PAIR_MIN_FILTERED_URLS = int(os.environ.get('PAIR_MIN_FILTERED_URLS', '3'))
PAIR_RESCUE_MAX_PER_PAIR = int(os.environ.get('PAIR_RESCUE_MAX_PER_PAIR', '8'))
MIN_RESULTS_RECALL_FALLBACK = int(os.environ.get('MIN_RESULTS_RECALL_FALLBACK', '15'))
if FOREIGN_FILTER_MODE not in {'strict', 'balanced'}:
    logger.warning("Invalid FOREIGN_FILTER_MODE=%s; fallback to 'balanced'", FOREIGN_FILTER_MODE)
    FOREIGN_FILTER_MODE = 'balanced'

logger.info(f"Config: MIN_URLS_PER_PAIR={MIN_URLS_PER_PAIR}, MAX_URLS_TO_PROCESS={MAX_URLS_TO_PROCESS}, "
            f"MAX_QUERIES_TOTAL={MAX_QUERIES_TOTAL}, LEAD_PROCESS_WORKERS={LEAD_PROCESS_WORKERS}, "
            f"AI_RELEVANCE_TOP_N={AI_RELEVANCE_TOP_N}, AI_RELEVANCE_MIN_CONFIDENCE={AI_RELEVANCE_MIN_CONFIDENCE}, "
            f"MAX_URLS_PER_DOMAIN={MAX_URLS_PER_DOMAIN}, "
            f"PRECHECK_UNKNOWN_KEEP_RATIO={PRECHECK_UNKNOWN_KEEP_RATIO}, "
            f"PRECHECK_THIN_PENALTY={PRECHECK_THIN_PENALTY}, "
            f"FOREIGN_FILTER_MODE={FOREIGN_FILTER_MODE}, "
            f"PRECHECK_NEGATIVE_KEEP_RATIO={PRECHECK_NEGATIVE_KEEP_RATIO}, "
            f"PRECHECK_FALLBACK_MIN_RATIO={PRECHECK_FALLBACK_MIN_RATIO}, "
            f"PAIR_MIN_FILTERED_URLS={PAIR_MIN_FILTERED_URLS}, "
            f"PAIR_RESCUE_MAX_PER_PAIR={PAIR_RESCUE_MAX_PER_PAIR}, "
            f"MIN_RESULTS_RECALL_FALLBACK={MIN_RESULTS_RECALL_FALLBACK}")

# Search progress tracking
search_progress = {
    'current': 0,
    'total': 0,
    'status': 'idle',
    'message': '',
    'results': []
}


def build_exclude_clause() -> str:
    """
    Build a -site: exclusion clause for search queries.
    Returns string like: " -site:ameblo.jp -site:note.com ..."
    """
    # Use top priority domains to avoid query length limits
    priority_excludes = [
        'ameblo.jp', 'note.com', 'hotpepper.jp',
        'beauty.rakuten.co.jp', 'tabelog.com', 'epark.jp', 'indeed.com',
        'goo.gl', 'ekiten.jp', 'rakuten.co.jp'
    ]
    return ' ' + ' '.join(f'-site:{d}' for d in priority_excludes)


def is_blocked_url(url: str) -> bool:
    """
    Check if URL should be blocked based on domain or path patterns.
    Returns True if URL should be excluded.
    """
    if not url:
        return True

    try:
        parsed = urlparse(url.lower())
        domain = parsed.netloc.replace('www.', '')
        path = parsed.path

        # Check exact domain match
        if domain in EXCLUDED_DOMAINS:
            return True

        # Check subdomain match
        for excluded in EXCLUDED_DOMAINS:
            if domain.endswith('.' + excluded) or domain == excluded:
                return True

        # Block Google Maps URLs
        if 'google.com' in domain and '/maps' in path:
            return True
        if domain == 'goo.gl' and '/maps' in path:
            return True
        if 'maps.google' in domain:
            return True

        # Block common directory patterns
        directory_patterns = [
            '/ranking', '/review', '/口コミ', '/評判',
            '/compare', '/比較', '/おすすめ', '/一覧'
        ]
        for pattern in directory_patterns:
            if pattern in path:
                return True

        return False

    except Exception:
        return True


def build_pass1_queries(city: str, btype: str) -> List[str]:
    """
    Build PASS 1 (coarse) queries - minimum queries to run first.

    These are the core queries that should find most actionable sites.
    Includes JP-biased variants to avoid overseas results.

    Args:
        city: City name
        btype: Business type

    Returns:
        List of query strings (with exclusion clauses)
    """
    exclude_clause = build_exclude_clause()
    queries = [
        f'{city} {btype}{exclude_clause}',
        f'{city} {btype} 料金{exclude_clause}',
        f'{city} {btype} 予約{exclude_clause}',
        f'{city} {btype} site:.jp{exclude_clause}',
        f'{city} {btype} 公式{exclude_clause}',
        f'{city} {btype} 口コミ{exclude_clause}',
        f'{city} {btype} おすすめ{exclude_clause}',
        f'{city} {btype} ホームページ{exclude_clause}',
        f'{city} {btype} 評判{exclude_clause}',
        f'{city} {btype} 人気{exclude_clause}',
        f'{city} {btype} 一覧{exclude_clause}',
        f'{city}駅 {btype}{exclude_clause}',
        f'{city} 周辺 {btype}{exclude_clause}',
        f'{city} {btype} 営業時間{exclude_clause}',
        f'{city} {btype} アクセス{exclude_clause}',
    ]
    return queries


def build_pass2_queries(city: str, btype: str) -> List[str]:
    """
    Build PASS 2 (expanded) queries - only if Pass 1 yields too few URLs.

    These queries target solo/small practitioners and specific platforms.
    Run only when MIN_URLS_PER_PAIR is not met after Pass 1.

    Args:
        city: City name
        btype: Business type

    Returns:
        List of query strings
    """
    exclude_clause = build_exclude_clause()
    queries = [
        # Solo/small business signals
        f'{city} {btype} 個人{exclude_clause}',
        f'{city} {btype} 自宅{exclude_clause}',
        f'{city} {btype} 完全予約制{exclude_clause}',
        f'{city} {btype} 女性専用{exclude_clause}',
        f'{city} {btype} 個人経営{exclude_clause}',
        f'{city} {btype} 出張{exclude_clause}',
        f'{city} {btype} オンライン{exclude_clause}',
        f'{city} {btype} アットホーム{exclude_clause}',
        f'{city} {btype} 隠れ家{exclude_clause}',
        # Additional solo signals
        f'{city} {btype} 個人サロン{exclude_clause}',
        f'{city} {btype} マンツーマン{exclude_clause}',
        f'{city} {btype} 少人数{exclude_clause}',
        f'{city} {btype} 予約制{exclude_clause}',
        f'{city} {btype} プライベート{exclude_clause}',
        f'{city} {btype} フリーランス{exclude_clause}',
        f'{city} {btype} 1対1{exclude_clause}',
        f'{city} {btype} 自宅サロン{exclude_clause}',
        f'{city} {btype} 初回{exclude_clause}',
        f'{city} {btype} 体験{exclude_clause}',
        # JP-biased solo signals
        f'{city} {btype} 個人サロン site:.jp{exclude_clause}',
        f'{city} {btype} アクセス 営業時間{exclude_clause}',
        # Platform-specific (no exclusion clause - targeting specific sites)
        f'{city} {btype} site:peraichi.com',
        f'{city} {btype} site:jimdofree.com',
        f'{city} {btype} site:wixsite.com',
        f'{city} {btype} site:studio.site',
        f'{city} {btype} site:wordpress.com',
        f'{city} {btype} site:wix.com',
        f'{city} {btype} site:goope.jp',
        f'{city} {btype} site:strikingly.com',
    ]

    # Add spiritual-specific queries for spiritual business types
    if btype in SPIRITUAL_BUSINESS_TYPES:
        queries.extend([
            f'{city} {btype} 個人セッション{exclude_clause}',
            f'{city} {btype} 遠隔{exclude_clause}',
        ])

    return queries


def build_pass3_queries(city: str, btype: str) -> List[str]:
    """
    Build PASS 3 (variation) queries - city name variations and comparison patterns.

    These broaden coverage by using different city name forms and
    review/comparison search terms that attract different result sets.

    Args:
        city: City name
        btype: Business type

    Returns:
        List of query strings
    """
    exclude_clause = build_exclude_clause()
    queries = []

    # City name variations
    if not city.endswith('市'):
        queries.append(f'{city}市 {btype}{exclude_clause}')
    if not city.endswith('駅'):
        queries.append(f'{city}駅前 {btype}{exclude_clause}')

    # Review/comparison/affordable patterns
    queries.extend([
        f'{city} {btype} 安い{exclude_clause}',
        f'{city} {btype} 初心者{exclude_clause}',
        f'{city} {btype} 相談{exclude_clause}',
        f'{city} {btype} 比較{exclude_clause}',
        f'{city} {btype} ランキング{exclude_clause}',
        f'{city} {btype} サイト{exclude_clause}',
    ])

    return queries


def prioritize_urls(urls: List[str]) -> List[str]:
    """
    Prioritize URLs for processing order.

    Prefer:
    1. Own-domain sites (not hosted on builder platforms)
    2. Short paths (likely homepage or main pages)
    3. URLs with business-related keywords in path

    Args:
        urls: List of URL strings

    Returns:
        Sorted list of URLs (highest priority first)
    """
    # Builder platform domains (lower priority - more likely to be thin sites)
    builder_domains = {
        'peraichi.com', 'jimdofree.com', 'wixsite.com', 'wordpress.com',
        'crayonsite.com', 'crayonsite.net', 'studio.site', 'webnode.jp',
        'weebly.com', 'strikingly.com', 'goope.jp',
    }

    # Business-related path keywords (higher priority)
    business_paths = {'/menu', '/price', '/service', '/access', '/contact', '/about', '/profile'}

    def url_priority(url: str) -> tuple:
        """Return priority tuple (lower = higher priority)."""
        try:
            parsed = urlparse(url.lower())
            domain = parsed.netloc.replace('www.', '')
            path = parsed.path.rstrip('/')

            # Priority 0: JP domain boost (.jp gets -2, foreign gets +2)
            if domain.endswith('.jp'):
                jp_score = -2
            else:
                jp_score = 2

            # Priority 1: Own domain vs builder platform
            is_builder = 0
            for bd in builder_domains:
                if bd in domain:
                    is_builder = 1
                    break

            # Priority 2: Path length (shorter = better)
            # Root path gets 0, others get count of segments
            if path == '' or path == '/':
                path_depth = 0
            else:
                path_depth = path.count('/')

            # Priority 3: Business-related paths get bonus (negative = higher priority)
            has_business_path = 0
            for bp in business_paths:
                if bp in path:
                    has_business_path = -1
                    break

            return (jp_score, is_builder, has_business_path, path_depth)
        except Exception:
            return (2, 1, 0, 99)  # Low priority for unparseable URLs

    return sorted(urls, key=url_priority)


# ============================================================
# URL PRE-FILTERING (before crawling)
# Reduces noise/404s and limits per-domain URLs
# ============================================================

# Foreign TLDs to hard-exclude (non-Japanese sites)
FOREIGN_TLDS = {
    '.de', '.fr', '.es', '.it', '.nl', '.pt', '.pl', '.ru', '.se',
    '.no', '.dk', '.fi', '.cz', '.at', '.ch', '.be', '.ie', '.gr',
    '.hu', '.ro', '.bg', '.hr', '.sk', '.si', '.lt', '.lv', '.ee',
    '.co.uk', '.org.uk', '.me.uk',
    '.com.au', '.com.br', '.com.mx', '.com.ar', '.com.co',
    '.com.tr', '.com.sa', '.com.eg', '.co.za', '.co.ke',
    '.co.in', '.co.id', '.co.th', '.co.kr', '.com.cn', '.com.tw',
    '.com.hk', '.com.sg', '.com.my', '.com.ph', '.com.vn',
    '.in', '.cn', '.kr', '.tw', '.th', '.vn', '.ph', '.my', '.sg', '.id',
}

# Allowlist: .com/.net/.org domains known to host JP businesses
JP_PLATFORM_ALLOWLIST = {
    'peraichi.com', 'jimdofree.com', 'wixsite.com', 'wordpress.com',
    'studio.site', 'crayonsite.com', 'crayonsite.net', 'webnode.jp',
    'weebly.com', 'strikingly.com', 'goope.jp', 'shopify.com',
    'jimdo.com', 'wix.com', 'squarespace.com', 'base.shop',
    'stores.jp', 'thebase.in', 'booth.pm',
    'google.com',  # Google Business Profile etc.
}


def is_foreign_url(url: str) -> bool:
    """
    Check if URL belongs to a foreign (non-JP) domain.
    Returns True if the domain has a foreign TLD and is not in the JP platform allowlist.
    """
    try:
        parsed = urlparse(url.lower())
        domain = parsed.netloc.replace('www.', '')

        # Allow .jp domains (all variants: .co.jp, .or.jp, .ne.jp, .ac.jp, etc.)
        if domain.endswith('.jp'):
            return False

        # Allow known JP platforms
        for allowed in JP_PLATFORM_ALLOWLIST:
            if domain == allowed or domain.endswith('.' + allowed):
                return False

        # Check foreign TLDs
        for tld in FOREIGN_TLDS:
            if domain.endswith(tld):
                return True

        # .com/.net/.org without JP platform allowlist - allow them
        # (they might be JP businesses with international domains)
        return False

    except Exception:
        return False


JP_URL_SIGNALS = [
    'japan', 'tokyo', 'osaka', 'kyoto', 'nagoya', 'yokohama',
    'sapporo', 'fukuoka', 'sendai', 'hiroshima', 'kobe',
    '/jp/', '/ja/', '-jp', '_jp'
]


def has_jp_signal_in_url(url: str) -> bool:
    """
    Heuristic rescue for foreign-TLD URLs that still look JP-local.
    Keeps weak-but-relevant candidates in balanced mode.
    """
    try:
        u = url.lower()
        parsed = urlparse(u)
        domain = parsed.netloc.replace('www.', '')
        path_q = f"{parsed.path}?{parsed.query}"

        if domain.startswith('jp.') or '.jp.' in domain or 'japan' in domain:
            return True
        if any(sig in u for sig in JP_URL_SIGNALS):
            return True
        if re.search(r'%e[3-9]', path_q):
            return True
        return False
    except Exception:
        return False


# File extensions to drop (non-HTML resources)
JUNK_EXTENSIONS = {
    '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg',
    '.mp4', '.mp3', '.wav', '.avi', '.mov',
    '.zip', '.rar', '.gz', '.tar',
    '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
    '.css', '.js', '.xml', '.json',
}

# Path patterns that indicate non-lead pages
JUNK_PATH_PATTERNS = [
    '/tag/', '/tags/',
    '/category/', '/categories/',
    '/wp-content/', '/wp-includes/', '/wp-admin/',
    '/search', '/search/',
    '?s=',
    'utm_', 'fbclid', 'gclid',
    '/page/', '/feed/',
    '/comment/', '/comments/',
    '/archive/', '/archives/',
    '/author/',
    '/login', '/signup', '/register',
    '/cart', '/checkout',
]

# Preferred paths for homepage/about selection (prioritize these when capping)
PREFERRED_PATH_PATTERNS = [
    '/',           # Homepage
    '/index',
    '/about',
    '/profile',
    '/concept',
    '/company',
    '/contact',
    '/access',
    '/menu',
    '/service',
    '/price',
]


def domain_key(url: str) -> str:
    """
    Extract domain key from URL for grouping.
    Removes 'www.' prefix for consistency.

    Args:
        url: URL string

    Returns:
        Domain string (lowercase, no www)
    """
    try:
        parsed = urlparse(url.lower())
        domain = parsed.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception:
        return ''


def normalize_url(url: str) -> str:
    """
    Normalize URL by removing fragments and trailing slashes.

    Args:
        url: URL string

    Returns:
        Normalized URL string
    """
    try:
        # Remove fragment
        if '#' in url:
            url = url.split('#')[0]
        # Remove trailing slash (except for root)
        if url.endswith('/') and url.count('/') > 3:
            url = url.rstrip('/')
        return url
    except Exception:
        return url


def is_junk_url(url: str) -> bool:
    """
    Check if URL is junk (non-HTML resource or known bad pattern).

    Args:
        url: URL string

    Returns:
        True if URL should be dropped
    """
    try:
        parsed = urlparse(url.lower())
        path = parsed.path
        query = parsed.query

        # Check file extension
        for ext in JUNK_EXTENSIONS:
            if path.endswith(ext):
                return True

        # Check path patterns
        full_url_lower = url.lower()
        for pattern in JUNK_PATH_PATTERNS:
            if pattern in path or pattern in query or pattern in full_url_lower:
                return True

        return False
    except Exception:
        return True  # If we can't parse it, drop it


def get_path_priority(url: str) -> int:
    """
    Get priority score for URL path (lower = higher priority).
    Used to prefer homepage/about/contact when capping per domain.

    Args:
        url: URL string

    Returns:
        Priority score (0 = highest priority)
    """
    try:
        parsed = urlparse(url.lower())
        path = parsed.path.rstrip('/')

        # Exact root
        if path == '' or path == '/':
            return 0

        # Check preferred patterns
        for i, pattern in enumerate(PREFERRED_PATH_PATTERNS):
            if pattern in path:
                return i + 1

        # Default priority for other paths
        return 100
    except Exception:
        return 999


def filter_for_japanese_urls(urls, url_titles: dict) -> Tuple[set, dict]:
    """
    Filter URL set to keep only Japanese/uncertain URLs, blocking overseas.

    Args:
        urls: Set or iterable of URL strings
        url_titles: Dict mapping URL -> search result title

    Returns:
        (filtered_urls_set, stats_dict)
    """
    kept = set()
    stats = {'total': 0, 'japanese': 0, 'overseas_blocked': 0, 'uncertain_kept': 0}

    for url in urls:
        stats['total'] += 1
        title = url_titles.get(url, '')
        classification = classify_url_japanese(url, title)

        if classification == 'overseas':
            stats['overseas_blocked'] += 1
        elif classification == 'japanese':
            stats['japanese'] += 1
            kept.add(url)
        else:  # uncertain
            stats['uncertain_kept'] += 1
            kept.add(url)

    stats['kept'] = len(kept)
    logger.info(f"[JP-FILTER] Total: {stats['total']}, Japanese: {stats['japanese']}, "
                f"Overseas blocked: {stats['overseas_blocked']}, Uncertain kept: {stats['uncertain_kept']}")
    return kept, stats


def prefilter_urls(urls, max_per_domain: int = 5) -> List[str]:
    """
    Pre-filter URLs before crawling to reduce noise and 404s.

    Steps:
    1. Normalize URLs (remove fragments, trailing slashes)
    2. Drop junk URLs (extensions, bad path patterns)
    3. Hard-exclude blocked domains (including ameblo)
    4. Cap per-domain (prefer homepage/about/contact)

    Args:
        urls: Iterable of URL strings
        max_per_domain: Maximum URLs to keep per domain (default 5)

    Returns:
        List of filtered URLs
    """
    # Step 1: Normalize and dedupe
    normalized = set()
    url_list = []
    for url in urls:
        norm = normalize_url(url)
        if norm and norm not in normalized:
            normalized.add(norm)
            url_list.append(norm)

    logger.info(f"[prefilter] Raw URLs: {len(urls)} -> Normalized unique: {len(url_list)}")

    # Step 2: Drop junk URLs
    non_junk = []
    junk_count = 0
    for url in url_list:
        if is_junk_url(url):
            junk_count += 1
        else:
            non_junk.append(url)

    logger.info(f"[prefilter] After junk removal: {len(non_junk)} (dropped {junk_count} junk)")

    # Step 3: Hard-exclude blocked domains (including ameblo)
    # Use the existing EXCLUDED_DOMAINS list + extra ameblo patterns
    blocked_patterns = ['ameblo']

    filtered = []
    blocked_count = 0
    for url in non_junk:
        domain = domain_key(url)
        url_lower = url.lower()

        # Check if domain is in EXCLUDED_DOMAINS
        is_blocked = False
        if domain in EXCLUDED_DOMAINS:
            is_blocked = True
        else:
            # Check subdomain match
            for excluded in EXCLUDED_DOMAINS:
                if domain.endswith('.' + excluded):
                    is_blocked = True
                    break

        # Extra check for ameblo patterns anywhere in URL
        if not is_blocked:
            for pattern in blocked_patterns:
                if pattern in url_lower:
                    is_blocked = True
                    break

        if is_blocked:
            blocked_count += 1
        else:
            filtered.append(url)

    logger.info(f"[prefilter] After domain exclusion: {len(filtered)} (blocked {blocked_count})")

    # Step 3.5: Foreign-TLD handling
    # strict   : remove all foreign TLD URLs
    # balanced : keep foreign URLs only when JP-local signals are detected
    jp_filtered = []
    foreign_count = 0
    rescued_foreign = 0
    for url in filtered:
        if not is_foreign_url(url):
            jp_filtered.append(url)
            continue

        foreign_count += 1
        if FOREIGN_FILTER_MODE == 'balanced' and has_jp_signal_in_url(url):
            jp_filtered.append(url)
            rescued_foreign += 1

    if foreign_count > 0:
        removed_foreign = foreign_count - rescued_foreign
        logger.info(
            "[prefilter] Foreign filter mode=%s: kept %d (rescued %d), removed %d",
            FOREIGN_FILTER_MODE,
            len(jp_filtered),
            rescued_foreign,
            removed_foreign
        )
    filtered = jp_filtered

    # Step 4: Per-domain cap (prefer homepage/about/contact paths)
    domain_urls = defaultdict(list)
    for url in filtered:
        domain = domain_key(url)
        if domain:
            domain_urls[domain].append(url)

    final_urls = []
    capped_count = 0
    for domain, urls_for_domain in domain_urls.items():
        if len(urls_for_domain) <= max_per_domain:
            final_urls.extend(urls_for_domain)
        else:
            # Sort by path priority (prefer homepage/about/contact)
            sorted_urls = sorted(urls_for_domain, key=get_path_priority)
            final_urls.extend(sorted_urls[:max_per_domain])
            capped_count += len(urls_for_domain) - max_per_domain

    logger.info(f"[prefilter] After per-domain cap ({max_per_domain}): {len(final_urls)} (capped {capped_count})")
    logger.info(f"[prefilter] Final: {len(final_urls)} URLs from {len(domain_urls)} domains")

    return final_urls


def precheck_url(url: str, timeout: float = 3.0) -> Tuple[str, int, Dict]:
    """
    Perform lightweight precheck on a URL to assess actionability.

    Returns:
        Tuple of (url, precheck_score, signals_dict)
    """
    signals = {
        'has_contact': False,
        'has_booking': False,
        'has_profile': False,
        'has_pricing': False,
        'is_thin': False,
        'fetch_ok': False,
        'text_length': 0,
    }

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'ja,en;q=0.9',
        }

        # Fetch with size limit (200KB max)
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout,
            stream=True,
            allow_redirects=True
        )

        if response.status_code >= 400:
            return url, -1, signals

        # Read limited content
        content = b''
        for chunk in response.iter_content(chunk_size=8192):
            content += chunk
            if len(content) > 200 * 1024:  # 200KB limit
                break

        try:
            text = content.decode('utf-8', errors='ignore')
        except:
            text = content.decode('shift_jis', errors='ignore')

        text_lower = text.lower()
        signals['fetch_ok'] = True

        # Strip HTML tags for text length check
        clean_text = re.sub(r'<[^>]+>', ' ', text)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        signals['text_length'] = len(clean_text)

        # Contact signals
        contact_patterns = [
            'お問い合わせ', 'contact', 'フォーム', 'form',
            r'[\w\.-]+@[\w\.-]+\.\w+',  # Email pattern
            r'\d{2,4}-\d{2,4}-\d{4}',   # Phone pattern
            'tel:', 'mailto:'
        ]
        for pattern in contact_patterns:
            if re.search(pattern, text_lower):
                signals['has_contact'] = True
                break

        # Booking signals
        booking_patterns = ['予約', '予約フォーム', 'ネット予約', 'オンライン予約', 'booking', 'reserve']
        signals['has_booking'] = any(p in text_lower for p in booking_patterns)

        # Profile signals
        profile_patterns = ['プロフィール', 'profile', 'about', '自己紹介', '経歴', 'ご挨拶']
        signals['has_profile'] = any(p in text_lower for p in profile_patterns)

        # Pricing signals
        pricing_patterns = ['料金', '価格', '円', '¥', 'price', 'メニュー']
        signals['has_pricing'] = any(p in text_lower for p in pricing_patterns)

        # Thin content check
        signals['is_thin'] = signals['text_length'] < 400

        # Calculate precheck score
        score = 0
        if signals['has_contact']:
            score += 2
        if signals['has_booking']:
            score += 1
        if signals['has_profile']:
            score += 1
        if signals['has_pricing']:
            score += 1
        if (
            signals['is_thin']
            and not signals['has_contact']
            and not signals['has_profile']
            and not signals['has_booking']
            and not signals['has_pricing']
        ):
            score -= PRECHECK_THIN_PENALTY

        return url, score, signals

    except requests.exceptions.Timeout:
        return url, 0, signals  # Unknown, keep with neutral score
    except requests.exceptions.RequestException:
        return url, 0, signals  # Network error, keep with neutral score
    except Exception as e:
        logger.debug(f"Precheck error for {url}: {e}")
        return url, 0, signals


def precheck_urls(urls: Set[str], max_workers: int = 10) -> Tuple[Set[str], Dict]:
    """
    Perform precheck on a set of URLs in parallel.

    Returns:
        Tuple of (kept_urls, stats_dict)
    """
    stats = {
        'total': len(urls),
        'checked': 0,
        'kept_good': 0,
        'kept_unknown': 0,
        'kept_negative': 0,
        'dropped': 0,
    }

    kept_urls = set()
    unknown_urls = []
    negative_urls = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(precheck_url, url): url for url in urls}

        for future in as_completed(futures):
            try:
                url, score, signals = future.result()
                stats['checked'] += 1

                if score >= 1:
                    # Good signal - keep
                    kept_urls.add(url)
                    stats['kept_good'] += 1
                elif score == 0:
                    # Unknown/unfetched - keep but track separately
                    unknown_urls.append(url)
                else:
                    # Negative score - keep a small configurable slice for recall
                    negative_urls.append(url)

            except Exception as e:
                logger.debug(f"Future error: {e}")
                unknown_urls.append(futures[future])

    # Keep unknown URLs with configurable ratio to avoid dropping fragile small-business sites.
    unknown_ratio = max(0.0, min(1.0, PRECHECK_UNKNOWN_KEEP_RATIO))
    max_unknown = int(len(urls) * unknown_ratio)
    if unknown_urls and unknown_ratio > 0 and max_unknown == 0:
        max_unknown = 1
    for url in unknown_urls[:max_unknown]:
        kept_urls.add(url)
        stats['kept_unknown'] += 1

    stats['dropped'] += len(unknown_urls) - min(len(unknown_urls), max_unknown)

    # Keep some negative-scored URLs to avoid over-pruning weak-but-relevant sites.
    negative_ratio = max(0.0, min(1.0, PRECHECK_NEGATIVE_KEEP_RATIO))
    max_negative = int(len(urls) * negative_ratio)
    if negative_urls and negative_ratio > 0 and max_negative == 0:
        max_negative = 1
    for url in negative_urls[:max_negative]:
        kept_urls.add(url)
        stats['kept_negative'] += 1

    stats['dropped'] += len(negative_urls) - min(len(negative_urls), max_negative)

    return kept_urls, stats


def score_in_range(score, min_score, max_score) -> bool:
    """Apply optional score bounds (min <= score <= max when provided)."""
    if score is None:
        return False
    try:
        score_val = int(score)
    except (ValueError, TypeError):
        return False
    if min_score is not None and score_val < min_score:
        return False
    if max_score is not None and score_val > max_score:
        return False
    return True


def ensure_pair_minimum_urls(
    urls: Set[str],
    pairs: List[Tuple[str, str]],
    pair_urls: Dict[Tuple[str, str], Set[str]],
    min_per_pair: int = 2,
    max_rescue_per_pair: int = 4,
) -> Tuple[Set[str], Dict]:
    """
    Recover URLs for each city+btype pair when filtering becomes too aggressive.
    Applies the same hard filters and foreign policy before rescuing.
    """
    kept_urls = set(urls)
    min_keep = max(0, min_per_pair)
    max_rescue = max(0, max_rescue_per_pair)

    stats = {
        'pairs_total': len(pairs),
        'pairs_below_min': 0,
        'pairs_recovered': 0,
        'rescued_urls': 0,
    }

    if min_keep == 0:
        return kept_urls, stats

    for pair in pairs:
        raw_candidates = pair_urls.get(pair, set())
        if not raw_candidates:
            continue

        candidates = []
        seen = set()
        for raw_url in raw_candidates:
            u = normalize_url(raw_url)
            if not u or u in seen:
                continue
            seen.add(u)

            if is_junk_url(u) or is_blocked_url(u):
                continue
            if is_foreign_url(u):
                if FOREIGN_FILTER_MODE != 'balanced' or not has_jp_signal_in_url(u):
                    continue
            candidates.append(u)

        if not candidates:
            continue

        current_count = sum(1 for u in candidates if u in kept_urls)
        if current_count >= min_keep:
            continue

        stats['pairs_below_min'] += 1
        need = min_keep - current_count
        rescued_for_pair = 0

        for u in sorted(candidates, key=get_path_priority):
            if u in kept_urls:
                continue
            kept_urls.add(u)
            rescued_for_pair += 1
            stats['rescued_urls'] += 1
            if rescued_for_pair >= need or rescued_for_pair >= max_rescue:
                break

        if rescued_for_pair > 0:
            stats['pairs_recovered'] += 1

    return kept_urls, stats


def run_search_async(
    prefecture: str,
    cities: List[str],
    business_types: List[str],
    limit: int,
    min_score: int | None,
    max_score: int | None,
    solo_classes: List[str],
    solo_score_min: int | None,
    solo_score_max: int | None,
    min_weakness: int,
    search_id: str,
    use_ai_verify: bool = False,
    ai_top_n: int = 30,
    use_ai_relevance: bool = False,
    ai_relevance_top_n: int = 30,
):
    """
    Run search in background thread with 2-pass strategy.

    2-PASS SEARCH STRATEGY:
    - Pass 1: Run coarse + solo-intent queries
    - Pass 2: Only if Pass 1 yields < MIN_URLS_PER_PAIR, run expanded queries
    - Apply MAX_URLS_TO_PROCESS cap before crawling
    - Apply URL prioritization (own-domain, short paths first)
    """
    global search_progress

    def is_current_job() -> bool:
        """Check if this job is still the current one and not cancelled."""
        if search_progress.get('search_id') != search_id:
            return False
        if search_progress.get('status') == 'cancelled':
            return False
        return True

    try:
        if not is_current_job():
            return

        search_progress['status'] = 'running'
        search_progress['results'] = []

        # Build all city+btype pairs
        pairs = [(city, btype) for city in cities for btype in business_types]
        total_pairs = len(pairs)

        # Track URLs per pair and overall
        pair_urls: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
        all_urls = set()
        url_to_queries = defaultdict(list)

        # Track query counts for observability
        pass1_queries_run = 0
        pass2_queries_run = 0
        pass2_triggered = 0

        # Initialize search engine
        searcher = MultiEngineSearch()

        logger.info(f"Starting 2-pass search: {prefecture}, {len(cities)} cities, "
                    f"{len(business_types)} btypes, {total_pairs} pairs")
        logger.info(f"Config: MIN_URLS_PER_PAIR={MIN_URLS_PER_PAIR}, MAX_URLS_TO_PROCESS={MAX_URLS_TO_PROCESS}")

        # ============================================================
        # PASS 1: Coarse queries (minimum queries per pair)
        # ============================================================
        search_progress['message'] = f"Pass 1: 基本検索中..."
        search_progress['total'] = total_pairs
        search_progress['current'] = 0

        for idx, (city, btype) in enumerate(pairs, 1):
            if not is_current_job():
                return

            search_progress['current'] = idx
            search_progress['message'] = f"Pass 1: {city} {btype} ({idx}/{total_pairs})"

            pass1_queries = build_pass1_queries(city, btype)
            for query in pass1_queries:
                if not is_current_job():
                    return
                try:
                    urls = searcher.search(query, max_results_per_engine=limit)
                    for url in urls:
                        url_to_queries[url].append(query)
                        pair_urls[(city, btype)].add(url)
                    all_urls.update(urls)
                    pass1_queries_run += 1
                except Exception as e:
                    logger.error(f"Pass 1 search error: {e}")
                    continue

            # Check if we've hit MAX_QUERIES_TOTAL
            if pass1_queries_run >= MAX_QUERIES_TOTAL:
                logger.info(f"Pass 1: Hit MAX_QUERIES_TOTAL ({MAX_QUERIES_TOTAL}), stopping early")
                break

        logger.info(f"Pass 1 complete: {pass1_queries_run} queries, {len(all_urls)} unique URLs")

        # ============================================================
        # PASS 2: Expanded queries (only for pairs with insufficient URLs)
        # ============================================================
        search_progress['message'] = f"Pass 2: 追加検索チェック中..."

        for idx, (city, btype) in enumerate(pairs, 1):
            if not is_current_job():
                return

            # Check if this pair needs more URLs
            pair_url_count = len(pair_urls[(city, btype)])
            if pair_url_count >= MIN_URLS_PER_PAIR:
                continue  # Sufficient URLs from Pass 1

            pass2_triggered += 1
            search_progress['message'] = f"Pass 2: {city} {btype} (不足: {pair_url_count}/{MIN_URLS_PER_PAIR})"

            pass2_queries = build_pass2_queries(city, btype)
            for query in pass2_queries:
                if not is_current_job():
                    return

                # Check query limit
                if pass1_queries_run + pass2_queries_run >= MAX_QUERIES_TOTAL:
                    logger.info(f"Pass 2: Hit MAX_QUERIES_TOTAL ({MAX_QUERIES_TOTAL}), stopping")
                    break

                try:
                    urls = searcher.search(query, max_results_per_engine=limit)
                    for url in urls:
                        url_to_queries[url].append(query)
                        pair_urls[(city, btype)].add(url)
                    all_urls.update(urls)
                    pass2_queries_run += 1
                except Exception as e:
                    logger.error(f"Pass 2 search error: {e}")
                    continue

                # Check if pair now has enough URLs
                if len(pair_urls[(city, btype)]) >= MIN_URLS_PER_PAIR:
                    break  # Move to next pair

            # Check overall query limit
            if pass1_queries_run + pass2_queries_run >= MAX_QUERIES_TOTAL:
                break

        logger.info(f"Pass 2 complete: {pass2_queries_run} queries, {pass2_triggered} pairs expanded")

        # ============================================================
        # PASS 3: Variation queries (city name variations, review/comparison)
        # Runs for ALL pairs if query budget remains
        # ============================================================
        pass3_queries_run = 0
        total_so_far = pass1_queries_run + pass2_queries_run

        if total_so_far < MAX_QUERIES_TOTAL:
            search_progress['message'] = f"Pass 3: バリエーション検索中..."

            for idx, (city, btype) in enumerate(pairs, 1):
                if not is_current_job():
                    return

                search_progress['message'] = f"Pass 3: {city} {btype} ({idx}/{total_pairs})"

                pass3_queries = build_pass3_queries(city, btype)
                for query in pass3_queries:
                    if not is_current_job():
                        return
                    if total_so_far + pass3_queries_run >= MAX_QUERIES_TOTAL:
                        break
                    try:
                        urls = searcher.search(query, max_results_per_engine=limit)
                        for url in urls:
                            url_to_queries[url].append(query)
                            pair_urls[(city, btype)].add(url)
                        all_urls.update(urls)
                        pass3_queries_run += 1
                    except Exception as e:
                        logger.error(f"Pass 3 search error: {e}")
                        continue

                if total_so_far + pass3_queries_run >= MAX_QUERIES_TOTAL:
                    break

        logger.info(f"Pass 3 complete: {pass3_queries_run} queries")
        logger.info(f"Total queries: {total_so_far + pass3_queries_run}, Total URLs: {len(all_urls)}")

        # Observability: Log collection stats
        total_collected = len(all_urls)
        logger.info(f"Total unique URLs collected: {total_collected}")

        # Update search progress with query stats
        search_progress['stats'] = search_progress.get('stats', {})
        search_progress['stats']['pass1_queries'] = pass1_queries_run
        search_progress['stats']['pass2_queries'] = pass2_queries_run
        search_progress['stats']['pass3_queries'] = pass3_queries_run
        search_progress['stats']['pass2_pairs_expanded'] = pass2_triggered

        # STEP 0.5: Japanese URL filter (block overseas sites before prefilter)
        search_progress['message'] = f"日本語フィルタリング中... ({len(all_urls)}件)"
        jp_filtered_urls, jp_filter_stats = filter_for_japanese_urls(all_urls, searcher.url_titles)
        logger.info(f"Japanese filter: {len(all_urls)} -> {len(jp_filtered_urls)} "
                     f"(blocked {jp_filter_stats['overseas_blocked']} overseas)")
        search_progress['stats']['japanese_filter'] = jp_filter_stats

        if not is_current_job():
            return

        # STEP 1: Pre-filter URLs (normalize, drop junk, exclude blocked domains, cap per-domain)
        search_progress['message'] = f"URLプリフィルタリング中... ({len(jp_filtered_urls)}件)"

        # Apply comprehensive prefilter (includes ameblo blocking, junk removal, domain cap)
        prefiltered_urls = prefilter_urls(jp_filtered_urls, max_per_domain=MAX_URLS_PER_DOMAIN)
        logger.info(f"Pre-filter: {len(jp_filtered_urls)} -> {len(prefiltered_urls)} URLs")

        # Update search progress stats
        search_progress['stats']['total_collected'] = total_collected
        search_progress['stats']['prefiltered'] = len(prefiltered_urls)

        if not is_current_job():
            return

        # STEP 2: Additional hard URL filtering (is_blocked_url for any remaining edge cases)
        search_progress['message'] = f"URLフィルタリング中... ({len(prefiltered_urls)}件)"
        blocked_count = 0
        filtered_urls = set()
        for url in prefiltered_urls:
            if is_blocked_url(url):
                blocked_count += 1
            else:
                filtered_urls.add(url)

        logger.info(f"Hard filter: {len(prefiltered_urls)} -> {len(filtered_urls)} (blocked {blocked_count})")

        if not is_current_job():
            return

        # STEP 2.5: AI Relevance Gate (optional - filters foreign/unrelated/portal sites)
        ai_relevance_stats = None
        if use_ai_relevance and len(filtered_urls) > 0:
            search_progress['message'] = f"AI関連性チェック中... (上位{ai_relevance_top_n}件)"
            try:
                from src.ai_verifier import AIVerifier
                relevance_verifier = AIVerifier()

                # Prioritize likely noisy URLs first (overseas/portal/sns/job), then check top N.
                def _ai_risk_rank(url: str) -> tuple:
                    u = (url or '').lower()
                    risk = 0
                    noisy_markers = [
                        'instagram.com', 'twitter.com', 'x.com', 'facebook.com', 'tiktok.com', 'youtube.com',
                        'lit.link', 'linktr.ee', 'hotpepper', 'tabelog', 'epark', 'rakuten',
                        'indeed', 'mynavi', 'doda', 'rikunabi',
                    ]
                    if any(m in u for m in noisy_markers):
                        risk += 3
                    foreign_tld_markers = [
                        '.de', '.fr', '.it', '.es', '.pt', '.nl', '.ru', '.pl', '.se', '.no', '.dk',
                        '.fi', '.ch', '.at', '.be', '.ie', '.cz', '.hu', '.ro', '.bg', '.hr',
                        '.sk', '.si', '.lt', '.lv', '.ee', '.br', '.mx', '.ar', '.cl', '.co',
                        '.pe', '.ve', '.cn', '.tw', '.kr', '.th', '.vn', '.ph', '.sg', '.my',
                        '.id', '.za', '.ng', '.ke', '.eg', '.au', '.nz', '.uk',
                    ]
                    if any(u.endswith(tld) for tld in foreign_tld_markers):
                        risk += 3
                    if '/blog' in u or '/ranking' in u or '/review' in u:
                        risk += 1
                    return (risk, len(u))

                ranked_urls = sorted(list(filtered_urls), key=_ai_risk_rank, reverse=True)
                urls_to_check = ranked_urls[:ai_relevance_top_n]
                urls_not_checked = ranked_urls[ai_relevance_top_n:]

                # Build metadata list (we only have URLs at this stage, no title/snippet)
                urls_with_meta = [{"url": u, "title": "", "snippet": ""} for u in urls_to_check]

                target_location = f"{prefecture}"
                kept_items, ai_relevance_stats = relevance_verifier.batch_verify_relevance(
                    urls_with_meta=urls_with_meta,
                    target_btypes=business_types,
                    target_location=target_location,
                    min_confidence=AI_RELEVANCE_MIN_CONFIDENCE,
                )

                kept_urls = {item["url"] for item in kept_items}
                filtered_urls = kept_urls | set(urls_not_checked)

                logger.info(f"AI relevance gate: checked {len(urls_to_check)}, "
                           f"kept {ai_relevance_stats.get('kept', 0)}, "
                           f"dropped {ai_relevance_stats.get('dropped', 0)}, "
                           f"by_category: {ai_relevance_stats.get('by_category', {})}")

            except ImportError as e:
                logger.warning(f"AI relevance skipped (openai not installed): {e}")
                ai_relevance_stats = {'error': 'openai not installed'}
            except Exception as e:
                logger.error(f"AI relevance gate failed: {e}")
                ai_relevance_stats = {'error': str(e)}

        search_progress['stats'] = search_progress.get('stats', {})
        search_progress['stats']['ai_relevance'] = ai_relevance_stats

        if not is_current_job():
            return

        # STEP 2.6: Pair-level rescue for recall (city+btype minimum URL guarantee)
        pair_recovery_stats = None
        if filtered_urls:
            recovered_urls, pair_recovery_stats = ensure_pair_minimum_urls(
                urls=filtered_urls,
                pairs=pairs,
                pair_urls=pair_urls,
                min_per_pair=PAIR_MIN_FILTERED_URLS,
                max_rescue_per_pair=PAIR_RESCUE_MAX_PER_PAIR,
            )
            if len(recovered_urls) != len(filtered_urls):
                logger.info(
                    "Pair recovery: %d -> %d URLs (rescued=%d, pairs_recovered=%d/%d)",
                    len(filtered_urls),
                    len(recovered_urls),
                    (pair_recovery_stats or {}).get('rescued_urls', 0),
                    (pair_recovery_stats or {}).get('pairs_recovered', 0),
                    (pair_recovery_stats or {}).get('pairs_total', 0),
                )
            filtered_urls = recovered_urls
        search_progress['stats']['pair_recovery'] = pair_recovery_stats

        # STEP 3: Mini precheck for actionability
        search_progress['message'] = f"サイト事前チェック中... ({len(filtered_urls)}件)"

        if len(filtered_urls) > 0:
            prechecked_urls, precheck_stats = precheck_urls(filtered_urls, max_workers=10)
            logger.info(f"Precheck stats: {precheck_stats}")
        else:
            prechecked_urls = filtered_urls
            precheck_stats = {'total': 0, 'kept_good': 0, 'kept_unknown': 0, 'dropped': 0}

        # Adaptive fallback: if precheck keeps too few, recover all filtered URLs.
        if len(filtered_urls) > 0:
            kept_ratio = len(prechecked_urls) / len(filtered_urls)
            precheck_stats['kept_ratio'] = round(kept_ratio, 3)
            if kept_ratio < max(0.0, min(1.0, PRECHECK_FALLBACK_MIN_RATIO)):
                recovered = len(filtered_urls) - len(prechecked_urls)
                prechecked_urls = set(filtered_urls)
                precheck_stats['fallback_recovered'] = recovered
                logger.warning(
                    "Precheck fallback activated: kept_ratio=%.3f < %.2f, recovered=%d URLs",
                    kept_ratio,
                    PRECHECK_FALLBACK_MIN_RATIO,
                    recovered,
                )

        # Update stats
        search_progress['stats']['precheck_kept'] = len(prechecked_urls)

        logger.info(f"After precheck: {len(prechecked_urls)} URLs (ready for prioritization)")

        if not is_current_job():
            return

        # STEP 4: Prioritize URLs (own-domain first, short paths first)
        search_progress['message'] = f"URL優先順位付け中... ({len(prechecked_urls)}件)"
        prioritized_urls = prioritize_urls(list(prechecked_urls))

        # STEP 5: Apply MAX_URLS_TO_PROCESS cap
        if len(prioritized_urls) > MAX_URLS_TO_PROCESS:
            logger.info(f"Capping URLs: {len(prioritized_urls)} -> {MAX_URLS_TO_PROCESS} (MAX_URLS_TO_PROCESS)")
            prioritized_urls = prioritized_urls[:MAX_URLS_TO_PROCESS]
        else:
            logger.info(f"URLs within cap: {len(prioritized_urls)} <= {MAX_URLS_TO_PROCESS}")

        search_progress['stats']['urls_to_process'] = len(prioritized_urls)
        search_progress['message'] = f"URL処理中... ({len(prioritized_urls)}件)"

        if not is_current_job():
            return

        # Process URLs with configured worker count
        processor = LeadProcessor(parallel_workers=LEAD_PROCESS_WORKERS, disable_progress=True)

        # Process with OSError protection - catch any stderr-related errors
        try:
            leads, failed_urls = processor.process_urls(prioritized_urls)
        except OSError as e:
            # OSError [Errno 22] can happen with tqdm on Windows in background threads
            logger.warning(f"OSError during URL processing (continuing): {e}")
            leads, failed_urls = [], prioritized_urls

        # Add source query info to leads
        for lead in leads:
            url = lead.get('url', '')
            source_queries = url_to_queries.get(url, [])
            if source_queries:
                lead['source_query'] = source_queries[0]
                lead['source_queries'] = source_queries[:3]

        # Post-crawl Japanese content check: remove leads with no Japanese content
        pre_jp_check = len(leads)
        jp_checked_leads = []
        non_jp_removed = 0
        for lead in leads:
            text = lead.get('visible_text', '') or ''
            title_text = lead.get('title', '') or ''
            name_text = lead.get('shop_name', '') or ''
            if has_japanese_content(text) or has_japanese_content(title_text) or has_japanese_content(name_text):
                lead['is_japanese'] = True
                jp_checked_leads.append(lead)
            else:
                non_jp_removed += 1
        leads = jp_checked_leads
        logger.info(f"Post-crawl JP check: {pre_jp_check} -> {len(leads)} (removed {non_jp_removed} non-JP)")
        search_progress['stats']['post_crawl_jp_removed'] = non_jp_removed

        # Deduplicate, filter, and apply scoring/weakness pipeline
        unique_leads = processor.deduplicate_leads(leads)
        kept_leads, _filtered_leads = processor.filter_and_boost(unique_leads)

        corp_filtered = sum(1 for lead in _filtered_leads if lead.get('filter_reason') == 'corporate_franchise')
        invalid_filtered = sum(1 for lead in _filtered_leads if str(lead.get('solo_classification', '')).lower() in ('invalid', 'blocked'))
        logger.info(f"Filtered leads: total={len(_filtered_leads)} corporate={corp_filtered} invalid/blocked={invalid_filtered}")

        # Optionally include filtered leads when user wants corporate/invalid/blocked
        candidate_leads = list(kept_leads)
        if any(c in solo_classes for c in ['corporate', 'invalid', 'blocked']):
            for lead in _filtered_leads:
                classification = str(lead.get('solo_classification', '')).lower()
                if classification in solo_classes:
                    candidate_leads.append(lead)
                    continue
                if 'corporate' in solo_classes and lead.get('filter_reason') == 'corporate_franchise':
                    lead['solo_classification'] = 'corporate'
                    if lead.get('solo_score') is None:
                        lead['solo_score'] = -999
                    candidate_leads.append(lead)

        def _passes_score_range(lead):
            classification = str(lead.get('solo_classification', '')).lower()
            if classification in ('invalid', 'blocked', 'corporate'):
                return True
            return score_in_range(lead.get('score', 0), min_score, max_score)

        # Apply score range (min <= score <= max when provided)
        filtered_leads = [
            lead for lead in candidate_leads
            if _passes_score_range(lead)
        ]

        def _solo_filter(lead):
            classification = str(lead.get('solo_classification', 'unknown'))
            if classification not in solo_classes:
                return False
            solo_score = lead.get('solo_score')
            if solo_score_min is not None or solo_score_max is not None:
                if solo_score is None or solo_score == '':
                    return False
                try:
                    solo_score_val = int(solo_score)
                except (ValueError, TypeError):
                    return False
                if solo_score_min is not None and solo_score_val < solo_score_min:
                    return False
                if solo_score_max is not None and solo_score_val > solo_score_max:
                    return False
            return True

        filtered_leads = [lead for lead in filtered_leads if _solo_filter(lead)]

        # Apply weakness filter (min_weakness threshold)
        if min_weakness > 0:
            filtered_leads = [
                lead for lead in filtered_leads
                if int(lead.get('weakness_score', 0)) >= min_weakness
            ]

        # Low-result recall fallback (independent of AI toggles):
        # if too few leads survive, recover UNKNOWN class first, then slightly relax min_score.
        fallback_target = max(0, MIN_RESULTS_RECALL_FALLBACK)
        if fallback_target > 0 and len(filtered_leads) < fallback_target:
            existing_urls = {str(l.get('url', '')) for l in filtered_leads if l.get('url')}
            remaining = [lead for lead in candidate_leads if str(lead.get('url', '')) not in existing_urls]

            # Stage 1: auto-include UNKNOWN leads when user limited to solo/small
            if len(filtered_leads) < fallback_target and 'unknown' not in solo_classes:
                rescued_unknown = []
                for lead in remaining:
                    classification = str(lead.get('solo_classification', 'unknown')).lower()
                    if classification in ('invalid', 'blocked', 'corporate'):
                        continue
                    if classification != 'unknown':
                        continue
                    if min_weakness > 0 and int(lead.get('weakness_score', 0)) < min_weakness:
                        continue
                    if min_score is not None and not score_in_range(lead.get('score', 0), min_score, max_score):
                        continue
                    rescued_unknown.append(lead)
                rescued_unknown.sort(key=lambda x: int(x.get('score', 0) or 0), reverse=True)
                need = fallback_target - len(filtered_leads)
                if need > 0:
                    filtered_leads.extend(rescued_unknown[:need])
                    logger.info(
                        "Recall fallback stage1: rescued %d unknown leads (target=%d, now=%d)",
                        min(need, len(rescued_unknown)),
                        fallback_target,
                        len(filtered_leads),
                    )
                    existing_urls = {str(l.get('url', '')) for l in filtered_leads if l.get('url')}
                    remaining = [lead for lead in candidate_leads if str(lead.get('url', '')) not in existing_urls]

            # Stage 2: if still too few, relax min_score by 15 points for non-corporate leads
            if len(filtered_leads) < fallback_target:
                relaxed_min = None
                if min_score is not None:
                    relaxed_min = max(0, int(min_score) - 15)

                rescued_relaxed = []
                for lead in remaining:
                    classification = str(lead.get('solo_classification', 'unknown')).lower()
                    if classification in ('invalid', 'blocked', 'corporate'):
                        continue
                    if min_weakness > 0 and int(lead.get('weakness_score', 0)) < min_weakness:
                        continue
                    if relaxed_min is not None and not score_in_range(lead.get('score', 0), relaxed_min, max_score):
                        continue
                    rescued_relaxed.append(lead)
                rescued_relaxed.sort(key=lambda x: int(x.get('score', 0) or 0), reverse=True)
                need = fallback_target - len(filtered_leads)
                if need > 0:
                    filtered_leads.extend(rescued_relaxed[:need])
                    logger.info(
                        "Recall fallback stage2: rescued %d relaxed-score leads (target=%d, now=%d, relaxed_min=%s)",
                        min(need, len(rescued_relaxed)),
                        fallback_target,
                        len(filtered_leads),
                        str(relaxed_min),
                    )

        # AI Relevance Filter (optional - post-crawl, runs BEFORE weakness verify)
        ai_filter_stats = None
        if use_ai_relevance and filtered_leads:
            search_progress['message'] = f"AIフィルタ中... (上位{ai_relevance_top_n}件)"
            try:
                from src.ai_verifier import AIVerifier
                filter_verifier = AIVerifier()
                filtered_leads, ai_filter_stats = filter_verifier.batch_filter_relevance(
                    leads=filtered_leads,
                    top_n=ai_relevance_top_n,
                    target_btypes=business_types,
                    target_location=prefecture,
                    min_confidence=AI_RELEVANCE_MIN_CONFIDENCE,
                )
                if 'error' not in (ai_filter_stats or {}):
                    logger.info(f"AI post-crawl filter: {ai_filter_stats}")
                    # Remove DROPped leads from further processing
                    filtered_leads = [
                        lead for lead in filtered_leads
                        if lead.get('ai_action') != 'DROP'
                    ]
            except ImportError as e:
                logger.warning(f"AI filter skipped (openai not installed): {e}")
                ai_filter_stats = {'error': 'openai not installed'}
            except Exception as e:
                logger.error(f"AI post-crawl filter failed: {e}")
                ai_filter_stats = {'error': str(e)}

        # AI Verification (optional - final step, after filter)
        ai_stats = None
        if use_ai_verify and filtered_leads:
            search_progress['message'] = f"AI検証中... (上位{ai_top_n}件)"
            try:
                from src.ai_verifier import AIVerifier
                verifier = AIVerifier()
                verified_leads, ai_stats = verifier.batch_verify(
                    filtered_leads,
                    top_n=ai_top_n,
                    min_confidence=6
                )
                if 'error' not in ai_stats:
                    logger.info(f"AI verification: {ai_stats}")
                    # Use leads with AI verification fields (sorted: weak first, strong last)
                    filtered_leads = verified_leads
            except ImportError as e:
                logger.warning(f"AI verification skipped (openai not installed): {e}")
                ai_stats = {'error': 'openai not installed'}
            except Exception as e:
                logger.error(f"AI verification failed: {e}")
                ai_stats = {'error': str(e)}

        class_counts = {}
        for lead in filtered_leads:
            key = str(lead.get('solo_classification', 'unknown'))
            class_counts[key] = class_counts.get(key, 0) + 1
        logger.info(f"Results by solo_classification: {class_counts}")

        logger.info(f"Found {len(filtered_leads)} leads (score >= {min_score})")

        # Final observability summary
        ai_rel_summary = ""
        if ai_relevance_stats and 'error' not in ai_relevance_stats:
            ai_rel_summary = f"""
AI relevance gate (pre-crawl): kept={ai_relevance_stats.get('kept', 0)}, dropped={ai_relevance_stats.get('dropped', 0)}
  By category: {ai_relevance_stats.get('by_category', {})}"""

        ai_filter_summary = ""
        if ai_filter_stats and 'error' not in ai_filter_stats:
            ai_filter_summary = f"""
AI filter (post-crawl): checked={ai_filter_stats.get('checked', 0)}, kept={ai_filter_stats.get('kept', 0)}, dropped={ai_filter_stats.get('dropped', 0)}
  By flag: {ai_filter_stats.get('by_flag', {})}"""

        logger.info(f"""
=== Search Summary (2-Pass) ===
Pass 1 queries: {pass1_queries_run}
Pass 2 queries: {pass2_queries_run} (triggered for {pass2_triggered} pairs)
Total URLs collected: {total_collected}
After prefilter: {len(prefiltered_urls)}
Blocked URLs removed: {blocked_count}
After hard filter: {len(filtered_urls)}{ai_rel_summary}{ai_filter_summary}
Precheck kept (good): {precheck_stats.get('kept_good', 0)}
Precheck kept (unknown): {precheck_stats.get('kept_unknown', 0)}
Precheck dropped: {precheck_stats.get('dropped', 0)}
URLs prioritized: {len(prioritized_urls)} (capped at {MAX_URLS_TO_PROCESS})
Final leads: {len(filtered_leads)}
================================
        """)

        # Save CSV - use absolute path
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        output_dir = os.path.join(base_dir, 'web_app', 'output')
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"leads_{prefecture}_{timestamp}.csv"
        output_path = os.path.join(output_dir, output_filename)
        OutputWriter.write_csv(filtered_leads, output_path)

        # Convert to JSON-friendly format
        results = []
        for lead in filtered_leads:
            weakness_reasons = lead.get('weakness_reasons', [])
            if isinstance(weakness_reasons, str):
                weakness_reasons = [r.strip() for r in weakness_reasons.split(';') if r.strip()]

            # Handle solo_reasons which may be a list or string
            solo_reasons = lead.get('solo_reasons', [])
            if isinstance(solo_reasons, str):
                solo_reasons = [r.strip() for r in solo_reasons.split(';') if r.strip()]

            # Handle solo_boost_reasons which may be a list or string
            solo_boost_reasons = lead.get('solo_boost_reasons', [])
            if isinstance(solo_boost_reasons, str):
                solo_boost_reasons = [r.strip() for r in solo_boost_reasons.split(';') if r.strip()]

            # Compute solo_score_100 and sales_label for JSON output
            score_val = int(lead.get('score', 0))
            solo_score_raw = lead.get('solo_score', None)
            solo_score_100 = max(0, min(100, int(solo_score_raw or 0) * 10))
            solo_class = str(lead.get('solo_classification', 'unknown')).lower()
            ws = int(lead.get('weakness_score', 0))
            ai_act = str(lead.get('ai_action', '')).upper()
            fr = str(lead.get('filter_reason', '')).lower()

            # Inline sales label logic (mirrors normalize.assign_sales_label)
            excl_pats = ['portal', 'sns', 'directory', 'blocked', 'overseas', 'foreign',
                         'job_listing', 'pdf_or_file', 'irrelevant']
            if ai_act == 'DROP':
                s_label, s_reason = '×', 'AIフィルタで除外'
            elif fr and any(p in fr for p in excl_pats):
                s_label, s_reason = '×', f'フィルタ除外: {lead.get("filter_reason", "")}'
            elif score_val >= 70 and ws >= 40 and solo_class in ('solo', 'small'):
                s_label, s_reason = '○', '高スコア＋弱いサイト＋個人/小規模'
            elif score_val >= 50 and ws >= 25:
                s_label, s_reason = '△', '中スコア＋一定の弱さ'
            else:
                s_label, s_reason = '×', '優先度低'

            raw_name = lead.get('shop_name', '') or '名称不明'

            results.append({
                'name': raw_name,
                'url': lead.get('url', ''),
                'lead_score': score_val,
                'score': score_val,
                'sales_label': s_label,
                'sales_reason': s_reason,
                'solo_score_100': solo_score_100,
                'grade': lead.get('grade', 'C'),
                'reasons': lead.get('reasons', ''),
                'site_type': lead.get('site_type', 'custom'),
                'business_type': lead.get('business_type', '不明'),
                'phone': lead.get('phone', ''),
                'city': lead.get('city', ''),
                'weakness_score': ws,
                'weakness_reasons': weakness_reasons[:2],
                'weakness_grade': lead.get('weakness_grade', 'C'),
                'solo_score': solo_score_raw,
                'solo_classification': lead.get('solo_classification', 'unknown'),
                'solo_reasons': solo_reasons[:5],
                'solo_evidence_snippets': lead.get('solo_evidence_snippets', []),
                'solo_detected_corp_terms': lead.get('solo_detected_corp_terms', []),
                'solo_boost': lead.get('solo_boost', 0),
                'solo_boost_reasons': solo_boost_reasons[:5],
                'url_status': lead.get('url_status', ''),
                'error_code': lead.get('error_code', ''),
                'source_query': lead.get('source_query', ''),
                # AI verification fields
                'ai_verified': lead.get('ai_verified', False),
                'ai_reason': lead.get('ai_reason', ''),
                'ai_confidence': lead.get('ai_confidence', 0),
                # AI filter fields (post-crawl relevance)
                'ai_action': lead.get('ai_action', ''),
                'ai_flags': lead.get('ai_flags', []),
                'ai_filter_reason': lead.get('ai_filter_reason', ''),
                'ai_filter_confidence': lead.get('ai_filter_confidence', 0),
            })

        # Sort by sales_label priority (○=2, △=1, ×=0), then lead_score desc
        _lp = {'○': 2, '△': 1, '×': 0}
        results.sort(
            key=lambda x: (_lp.get(x.get('sales_label', '×'), 0), x.get('lead_score', 0)),
            reverse=True
        )

        if not is_current_job():
            return

        search_progress['results'] = results
        search_progress['csv_path'] = output_filename  # Only store filename for download API
        search_progress['status'] = 'completed'
        search_progress['message'] = f"完了: {len(results)}件のリードを発見"

        # Add stats to progress for UI
        search_progress['stats'] = {
            'pass1_queries': pass1_queries_run,
            'pass2_queries': pass2_queries_run,
            'pass2_pairs_expanded': pass2_triggered,
            'total_collected': total_collected,
            'blocked_removed': blocked_count,
            'precheck_kept': precheck_stats.get('kept_good', 0) + precheck_stats.get('kept_unknown', 0),
            'precheck_dropped': precheck_stats.get('dropped', 0),
            'urls_processed': len(prioritized_urls),
            'final_leads': len(results),
            'ai_verified': use_ai_verify,
            'ai_stats': ai_stats,
            'ai_relevance_enabled': use_ai_relevance,
            'ai_relevance_stats': ai_relevance_stats,
            'ai_filter_stats': ai_filter_stats,
            'japanese_filter': jp_filter_stats,
            'post_crawl_jp_removed': non_jp_removed,
            # Pipeline funnel for debugging collection volume
            'funnel': {
                'queries_run': pass1_queries_run + pass2_queries_run + pass3_queries_run,
                'urls_collected': total_collected,
                'urls_after_jp_filter': jp_filter_stats.get('kept', total_collected),
                'jp_overseas_blocked': jp_filter_stats.get('overseas_blocked', 0),
                'urls_after_prefilter': search_progress.get('stats', {}).get('prefiltered', total_collected),
                'urls_after_hardfilter': total_collected - blocked_count,
                'urls_after_precheck': precheck_stats.get('kept_good', 0) + precheck_stats.get('kept_unknown', 0) + precheck_stats.get('kept_negative', 0),
                'urls_processed': len(prioritized_urls),
                'leads_after_jp_check': len(leads),
                'leads_final': len(results),
            },
        }

        logger.info(f"Search completed: {len(results)} results saved to {output_path}")

    except Exception as e:
        logger.error(f"Search failed: {e}", exc_info=True)
        if is_current_job():
            search_progress['status'] = 'error'
            search_progress['message'] = f"エラー: {str(e)}"


@app.route('/')
def index():
    """Main page."""
    return render_template('index.html',
                         regions=REGIONS,
                         business_types=BUSINESS_TYPES,
                         business_type_categories=BUSINESS_TYPE_CATEGORIES)


@app.route('/api/regions', methods=['GET'])
def api_regions():
    """Get all regions with prefectures."""
    return jsonify({
        'regions': REGIONS
    })


@app.route('/api/prefectures/<region>', methods=['GET'])
def api_prefectures(region):
    """Get prefectures for a region."""
    prefectures = get_prefectures_by_region(region)
    return jsonify({
        'region': region,
        'prefectures': prefectures
    })


@app.route('/api/cities/<prefecture>', methods=['GET'])
def api_cities(prefecture):
    """Get cities for a prefecture."""
    cities = get_cities_by_prefecture(prefecture)
    return jsonify({
        'prefecture': prefecture,
        'cities': cities
    })


@app.route('/api/business-types', methods=['GET'])
def api_business_types():
    """Get all business types organized by category."""
    return jsonify({
        'categories': BUSINESS_TYPE_CATEGORIES,
    })


@app.route('/api/search', methods=['POST'])
def api_search():
    """Start search in background."""
    global search_progress

    try:
        data = request.get_json()

        # Validate input
        prefecture = data.get('prefecture')  # Changed from region to prefecture
        cities = data.get('cities', [])
        business_types = data.get('business_types', [])

        # Validate limit: must be in [5, 10, 20, 50, 100], default to 10 if invalid
        VALID_LIMITS = {5, 10, 20, 50, 100}
        limit_raw = int(data.get('limit', 30))
        limit = limit_raw if limit_raw in VALID_LIMITS else 20

        min_score_raw = data.get('min_score', None)
        max_score_raw = data.get('max_score', None)
        min_score = int(min_score_raw) if min_score_raw not in (None, '') else None
        max_score = int(max_score_raw) if max_score_raw not in (None, '') else None
        solo_classes = data.get('solo_classifications', None)
        solo_score_min = data.get('solo_score_min', None)
        solo_score_max = data.get('solo_score_max', None)
        min_weakness_raw = data.get('min_weakness', 0)
        min_weakness = int(min_weakness_raw) if min_weakness_raw not in (None, '') else 0

        # AI verification parameters
        use_ai_verify = data.get('use_ai_verify', False)
        ai_top_n_raw = data.get('ai_top_n', 30)
        ai_top_n = int(ai_top_n_raw) if ai_top_n_raw not in (None, '') else 30

        # AI relevance gate parameters
        use_ai_relevance = data.get('use_ai_relevance', False)
        ai_relevance_top_n_raw = data.get('ai_relevance_top_n', AI_RELEVANCE_TOP_N)
        ai_relevance_top_n = int(ai_relevance_top_n_raw) if ai_relevance_top_n_raw not in (None, '') else AI_RELEVANCE_TOP_N

        if not isinstance(solo_classes, list) or not solo_classes:
            solo_classes = ['solo', 'small', 'unknown', 'corporate']
        logger.info("Search options: solo_classes=%s", solo_classes)
        try:
            solo_score_min = int(solo_score_min) if solo_score_min not in (None, '') else None
        except (ValueError, TypeError):
            solo_score_min = None
        try:
            solo_score_max = int(solo_score_max) if solo_score_max not in (None, '') else None
        except (ValueError, TypeError):
            solo_score_max = None

        if not prefecture or prefecture not in CITIES_BY_PREFECTURE:
            return jsonify({'status': 'error', 'message': '都道府県を選択してください'}), 400

        if not cities:
            return jsonify({'status': 'error', 'message': '都市を少なくとも1つ選択してください'}), 400

        if not business_types:
            return jsonify({'status': 'error', 'message': '業種を少なくとも1つ選択してください'}), 400

        if min_score is not None and (min_score < 0 or min_score > 100):
            return jsonify({'status': 'error', 'message': '最低スコアは0-100の範囲で指定してください'}), 400
        if max_score is not None and (max_score < 0 or max_score > 100):
            return jsonify({'status': 'error', 'message': '最高スコアは0-100の範囲で指定してください'}), 400

        if search_progress.get('status') in ('starting', 'running'):
            return jsonify({'status': 'error', 'message': '検索が実行中です。完了後に再実行してください。'}), 409

        # Save to session
        session['last_search'] = {
            'prefecture': prefecture,
            'cities': cities,
            'business_types': business_types,
            'limit': limit,
            'min_score': min_score,
            'max_score': max_score,
            'solo_classifications': solo_classes,
            'solo_score_min': solo_score_min,
            'solo_score_max': solo_score_max,
            'min_weakness': min_weakness,
            'use_ai_verify': use_ai_verify,
            'ai_top_n': ai_top_n,
            'use_ai_relevance': use_ai_relevance,
            'ai_relevance_top_n': ai_relevance_top_n,
        }

        # Reset progress
        search_id = uuid.uuid4().hex
        search_progress = {
            'current': 0,
            'total': 0,
            'status': 'starting',
            'message': '検索を開始しています...',
            'results': [],
            'search_id': search_id,
        }

        # Start background thread
        thread = threading.Thread(
            target=run_search_async,
            args=(
                prefecture,
                cities,
                business_types,
                limit,
                min_score,
                max_score,
                solo_classes,
                solo_score_min,
                solo_score_max,
                min_weakness,
                search_id,
                use_ai_verify,
                ai_top_n,
                use_ai_relevance,
                ai_relevance_top_n,
            )
        )
        thread.daemon = True
        thread.start()

        return jsonify({
            'status': 'started',
            'message': '検索を開始しました'
        })

    except Exception as e:
        logger.error(f"API search error: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/progress', methods=['GET'])
def api_progress():
    """Get current search progress."""
    return jsonify(search_progress)


@app.route('/api/cancel', methods=['POST'])
def api_cancel():
    """Cancel the current search."""
    global search_progress

    if search_progress.get('status') not in ('starting', 'running'):
        return jsonify({
            'status': 'error',
            'message': '実行中の検索がありません'
        }), 400

    # Set status to cancelled - the background thread will detect this
    search_progress['status'] = 'cancelled'
    search_progress['message'] = '検索が中止されました'
    logger.info("Search cancelled by user")

    return jsonify({
        'status': 'cancelled',
        'message': '検索を中止しました'
    })


@app.route('/api/download/<path:filename>', methods=['GET'])
def api_download(filename):
    """Download CSV file."""
    try:
        # Get absolute path to output directory
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        file_path = os.path.join(base_dir, 'web_app', 'output', filename)

        if not os.path.exists(file_path):
            return jsonify({'status': 'error', 'message': 'ファイルが見つかりません'}), 404

        return send_file(
            file_path,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        logger.error(f"Download error: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/test', methods=['GET'])
def api_test():
    """Test endpoint for debugging."""
    return jsonify({
        'status': 'ok',
        'regions': REGIONS,
        'total_prefectures': len(CITIES_BY_PREFECTURE),
        'business_types': BUSINESS_TYPES,
        'search_progress': search_progress,
        'excluded_domains_count': len(EXCLUDED_DOMAINS),
    })


if __name__ == '__main__':
    # Ensure output directory exists
    os.makedirs('web_app/output', exist_ok=True)
    os.makedirs('logs', exist_ok=True)

    # Run Flask app
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'

    logger.info(f"Starting Lead Finder Web App on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
