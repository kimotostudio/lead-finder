"""
Lead normalization module for clean, consistent CSV output.

Provides:
- URL normalization (https, remove tracking params, fragments, trailing slash)
- Text sanitization (newlines, spaces, character limits)
- Deduplication (by normalized URL, fallback to store_name+city)
- Schema mapping to final CSV format
"""
import re
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

logger = logging.getLogger(__name__)

# Final CSV schema (fixed column order)
# Liveness fields (M-P) are appended AFTER existing columns
FINAL_SCHEMA = [
    'display_name',    # A: 表示名 (= store_name alias)
    'store_name',      # B: Business/shop name (backward compat)
    'url',             # C: Normalized URL
    'lead_score',      # D: Primary score 0-100 (= score)
    'sales_label',     # E: 営業優先度 ○/△/×
    'sales_reason',    # F: 営業ラベル理由
    'comment',         # G: 1-line improvement note (max 60 chars)
    'score',           # H: Integer 0-100 (backward compat, = lead_score)
    # Debug / explainability fields
    'filter_reason',   # I: If filtered, the reason token
    'score_boost',     # J: Applied score delta (int)
    'boost_reasons',   # K: Short list of boost reason tokens
    'weakness_score',  # L: Weakness score (0-100)
    'weakness_grade',  # M: Weakness grade (A/B/C)
    'weakness_reasons',# N: Weakness reasons
    'solo_score',      # O: Solo proprietor likelihood score (raw)
    'solo_score_100',  # P: Solo score normalized to 0-100
    'solo_classification',  # Q: Solo classification
    'solo_reasons',    # R: Solo reasons
    'solo_evidence_snippets',  # S: Evidence snippets
    'solo_detected_corp_terms',# T: Corporate terms
    'solo_boost',      # U: Solo-based score boost
    'solo_boost_reasons', # V: Solo boost reason tokens
    'url_status',      # W: URL status (OK/INVALID/BLOCKED)
    'error_code',      # X: URL error code
    'region',          # Y: Region (e.g., 関東)
    'city',            # Z: City/ward
    'business_type',   # AA: Type of business
    'site_type',       # AB: Platform type (peraichi, ameblo, custom, etc.)
    'phone',           # AC: Phone number
    'email',           # AD: Email address
    'source_query',    # AE: Search query that found this lead
    'fetched_at_iso',  # AF: ISO timestamp when fetched
    # Liveness fields
    'http_status',     # AG: HTTP status code (int or empty)
    'final_url',       # AH: Final URL after redirects
    'is_alive',        # AI: Boolean - True if site is reachable
    'checked_at_iso',  # AJ: ISO timestamp when liveness was checked
    # AI Verification fields
    'ai_verified',     # AK: AI判定結果 (YES/空白)
    'ai_reason',       # AL: AI判定理由
    'ai_confidence',   # AM: AI確信度 (1-10)
    # AI Filter fields (post-crawl relevance)
    'ai_action',       # AN: KEEP/DROP
    'ai_flags',        # AO: フラグ (OVERSEAS, PORTAL, SNS, etc.)
    'ai_filter_reason',# AP: フィルタ理由
    'ai_filter_confidence', # AQ: フィルタ確信度 (1-10)
]

# Japanese header labels for CSV
HEADER_LABELS = {
    'display_name': '表示名',
    'store_name': '店舗名',
    'url': 'URL',
    'lead_score': 'リードスコア',
    'sales_label': '営業優先度',
    'sales_reason': '営業ラベル理由',
    'comment': 'コメント',
    'score': 'スコア',
    'filter_reason': 'フィルタ理由',
    'score_boost': 'スコア増分',
    'boost_reasons': '増分理由',
    'weakness_score': '弱みスコア',
    'weakness_grade': '弱みランク',
    'weakness_reasons': '弱み理由',
    'solo_score': '個人度スコア(raw)',
    'solo_score_100': '個人度スコア(0-100)',
    'solo_classification': '個人度分類',
    'solo_reasons': '個人度理由',
    'solo_evidence_snippets': '個人度根拠',
    'solo_detected_corp_terms': '法人語検出',
    'solo_boost': '個人ブースト',
    'solo_boost_reasons': '個人ブースト理由',
    'url_status': 'URL状態',
    'error_code': 'URLエラー',
    'region': '地方',
    'city': '市区町村',
    'business_type': '業種',
    'site_type': 'サイト種別',
    'phone': '電話番号',
    'email': 'メール',
    'source_query': '検索クエリ',
    'fetched_at_iso': '取得日時',
    # Liveness fields
    'http_status': 'HTTPステータス',
    'final_url': '最終URL',
    'is_alive': '生存',
    'checked_at_iso': 'チェック日時',
    # AI Verification fields
    'ai_verified': 'AI検証',
    'ai_reason': 'AI判定理由',
    'ai_confidence': 'AI確信度',
    # AI Filter fields
    'ai_action': 'AIフィルタ結果',
    'ai_flags': 'AIフィルタフラグ',
    'ai_filter_reason': 'AIフィルタ理由',
    'ai_filter_confidence': 'AIフィルタ確信度',
}

# Tracking params to remove from URLs
TRACKING_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'fbclid', 'gclid', 'gclsrc', 'dclid', 'msclkid',
    'ref', 'referer', 'referrer', 'source', 'affiliate',
    '_ga', '_gl', 'mc_cid', 'mc_eid',
}


def normalize_url_strict(url: str) -> str:
    """
    Strictly normalize URL for deduplication and clean output.

    Rules:
    - Ensure https:// scheme (upgrade http)
    - Remove URL fragments (#...)
    - Remove tracking query params (utm_*, fbclid, gclid, etc.)
    - Strip trailing slash (except for root domain)
    - Lowercase domain

    Args:
        url: Raw URL string

    Returns:
        Normalized URL string
    """
    if not url:
        return ''

    url = url.strip()

    # Add scheme if missing
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    # Upgrade http to https
    if url.startswith('http://'):
        url = 'https://' + url[7:]

    try:
        parsed = urlparse(url)

        # Lowercase domain, remove www prefix
        netloc = parsed.netloc.lower()
        if netloc.startswith('www.'):
            netloc = netloc[4:]

        # Filter out tracking params from query string
        if parsed.query:
            params = parse_qs(parsed.query, keep_blank_values=True)
            filtered_params = {
                k: v for k, v in params.items()
                if k.lower() not in TRACKING_PARAMS
            }
            query = urlencode(filtered_params, doseq=True) if filtered_params else ''
        else:
            query = ''

        # Clean path: remove trailing slash unless root
        path = parsed.path
        if path != '/' and path.endswith('/'):
            path = path.rstrip('/')
        if not path:
            path = '/'

        # Reconstruct URL without fragment
        normalized = urlunparse((
            'https',
            netloc,
            path,
            '',      # params
            query,   # filtered query
            ''       # no fragment
        ))

        return normalized

    except Exception as e:
        logger.warning(f"Failed to normalize URL '{url}': {e}")
        return url


def sanitize_text(text: str, max_length: Optional[int] = None) -> str:
    """
    Sanitize text field for clean CSV output.

    Rules:
    - Replace newlines with spaces
    - Collapse multiple spaces into one
    - Strip leading/trailing whitespace
    - Optionally truncate to max_length

    Args:
        text: Raw text string
        max_length: Optional maximum character length

    Returns:
        Sanitized text string
    """
    if not text:
        return ''

    # Convert to string if not already
    text = str(text)

    # Replace newlines and tabs with spaces
    text = re.sub(r'[\n\r\t]+', ' ', text)

    # Collapse multiple spaces
    text = re.sub(r' +', ' ', text)

    # Strip whitespace
    text = text.strip()

    # Truncate if needed
    if max_length and len(text) > max_length:
        text = text[:max_length-1] + '…'

    return text


def normalize_store_name(name: str) -> str:
    """
    Normalize store name for deduplication comparison.

    Rules:
    - Lowercase
    - Remove spaces, punctuation, symbols
    - Normalize Japanese characters (full-width to half-width numbers)

    Args:
        name: Store name

    Returns:
        Normalized name for comparison
    """
    if not name:
        return ''

    name = str(name).lower()

    # Remove common symbols and punctuation
    name = re.sub(r'[・\-_\s\.\,\!\?\'\"\(\)\[\]\{\}\/\\【】「」『』（）]', '', name)

    # Normalize full-width numbers to half-width
    fw_nums = '０１２３４５６７８９'
    hw_nums = '0123456789'
    for fw, hw in zip(fw_nums, hw_nums):
        name = name.replace(fw, hw)

    return name


def ensure_int_score(score) -> int:
    """
    Ensure score is an integer 0-100.

    Args:
        score: Raw score value (could be int, str, None)

    Returns:
        Integer score clamped to 0-100
    """
    if score is None:
        return 0

    try:
        score_int = int(score)
        return max(0, min(100, score_int))
    except (ValueError, TypeError):
        return 0


def ensure_int_optional(value):
    """
    Ensure value is an int if possible, otherwise empty string.

    Keeps negative values (e.g., -999 for corporate).
    """
    if value is None or value == '':
        return ''
    try:
        return int(value)
    except (ValueError, TypeError):
        return ''


def compute_solo_score_100(solo_score) -> int:
    """
    Normalize raw solo_score to 0-100 scale.
    solo_score_100 = min(100, solo_score * 10), clamped to 0-100.
    Returns 0 if solo_score is missing or negative.
    """
    if solo_score is None or solo_score == '':
        return 0
    try:
        raw = int(solo_score)
    except (ValueError, TypeError):
        return 0
    return max(0, min(100, raw * 10))


# Exclusion patterns in filter_reason that indicate the lead should be ×
_EXCLUSION_FILTER_PATTERNS = [
    'portal', 'sns', 'directory', 'blocked', 'overseas', 'foreign',
    'job_listing', 'pdf_or_file', 'irrelevant',
]


def assign_sales_label(lead: Dict) -> Tuple[str, str]:
    """
    Assign a sales priority label (○/△/×) and reason.

    Rules (evaluated top-to-bottom, first match wins):
    1. × if ai_action == 'DROP'
    2. × if filter_reason matches exclusion patterns
    3. ○ if lead_score >= 70 AND weakness_score >= 40 AND solo_classification in {solo, small}
    4. △ if lead_score >= 50 AND weakness_score >= 25
    5. × otherwise ("優先度低")

    Args:
        lead: Normalized lead dict (must have lead_score, weakness_score,
              solo_classification, ai_action, filter_reason)

    Returns:
        Tuple of (sales_label, sales_reason)
    """
    ai_action = str(lead.get('ai_action', '')).upper()
    filter_reason = str(lead.get('filter_reason', '')).lower()
    lead_score = int(lead.get('lead_score', 0) or 0)
    weakness_score = int(lead.get('weakness_score', 0) or 0)
    solo_class = str(lead.get('solo_classification', 'unknown')).lower()

    # Rule 1: AI dropped
    if ai_action == 'DROP':
        return ('×', 'AIフィルタで除外')

    # Rule 2: filter_reason exclusion
    if filter_reason:
        for pat in _EXCLUSION_FILTER_PATTERNS:
            if pat in filter_reason:
                return ('×', f'フィルタ除外: {lead.get("filter_reason", "")}')

    # Rule 3: High-priority ○
    if lead_score >= 70 and weakness_score >= 40 and solo_class in ('solo', 'small'):
        return ('○', '高スコア＋弱いサイト＋個人/小規模')

    # Rule 4: Medium-priority △
    if lead_score >= 50 and weakness_score >= 25:
        return ('△', '中スコア＋一定の弱さ')

    # Rule 5: Low priority ×
    return ('×', '優先度低')


def map_to_final_schema(raw_lead: Dict, source_query: str = '', region: str = '') -> Dict:
    """
    Map a raw lead dict to the final normalized schema.

    Args:
        raw_lead: Raw lead dictionary from processor
        source_query: Search query that found this lead
        region: Region name (e.g., 関東)

    Returns:
        Dictionary matching FINAL_SCHEMA
    """
    # Build comment from reasons (truncate to 60 chars)
    reasons = raw_lead.get('reasons', '')
    if reasons:
        # Convert semicolon-separated to comma-separated, sanitize
        comment = sanitize_text(reasons.replace(';', ', '), max_length=60)
    else:
        comment = ''

    weakness_reasons = raw_lead.get('weakness_reasons', '')
    if isinstance(weakness_reasons, list):
        weakness_reasons = '; '.join(weakness_reasons)

    solo_reasons = raw_lead.get('solo_reasons', '')
    if isinstance(solo_reasons, list):
        solo_reasons = '; '.join(solo_reasons)

    solo_snippets = raw_lead.get('solo_evidence_snippets', '')
    if isinstance(solo_snippets, list):
        solo_snippets = '; '.join(solo_snippets)

    solo_corp_terms = raw_lead.get('solo_detected_corp_terms', '')
    if isinstance(solo_corp_terms, list):
        solo_corp_terms = '; '.join(solo_corp_terms)

    # Handle solo_boost_reasons which may be a list
    solo_boost_reasons = raw_lead.get('solo_boost_reasons', '')
    if isinstance(solo_boost_reasons, list):
        solo_boost_reasons = '; '.join(solo_boost_reasons)

    store_name = sanitize_text(raw_lead.get('shop_name', '') or raw_lead.get('store_name', ''))

    score = ensure_int_score(raw_lead.get('score'))
    solo_score_raw = raw_lead.get('solo_score')
    solo_score_100 = compute_solo_score_100(solo_score_raw)

    result = {
        'display_name': store_name,
        'store_name': store_name,
        'url': normalize_url_strict(raw_lead.get('url', '')),
        'lead_score': score,
        'sales_label': '',   # Filled below
        'sales_reason': '',  # Filled below
        'comment': comment,
        'score': score,
        'filter_reason': sanitize_text(raw_lead.get('filter_reason', '')),
        'score_boost': int(raw_lead.get('score_boost', 0)) if raw_lead.get('score_boost', None) is not None else '',
        'boost_reasons': sanitize_text(raw_lead.get('boost_reasons', '')),
        # Weakness fields (may be added later by scoring pipeline)
        'weakness_score': ensure_int_score(raw_lead.get('weakness_score')),
        'weakness_grade': sanitize_text(raw_lead.get('weakness_grade', '')),
        'weakness_reasons': sanitize_text(weakness_reasons),
        'solo_score': ensure_int_optional(solo_score_raw),
        'solo_score_100': solo_score_100,
        'solo_classification': sanitize_text(raw_lead.get('solo_classification', '')),
        'solo_reasons': sanitize_text(solo_reasons),
        'solo_evidence_snippets': sanitize_text(solo_snippets),
        'solo_detected_corp_terms': sanitize_text(solo_corp_terms),
        'solo_boost': int(raw_lead.get('solo_boost', 0)) if raw_lead.get('solo_boost', None) is not None else '',
        'solo_boost_reasons': sanitize_text(solo_boost_reasons),
        'url_status': sanitize_text(raw_lead.get('url_status', '')),
        'error_code': sanitize_text(raw_lead.get('error_code', '')),
        'region': sanitize_text(region),
        'city': sanitize_text(raw_lead.get('city', '')),
        'business_type': sanitize_text(raw_lead.get('business_type', '')),
        'site_type': sanitize_text(raw_lead.get('site_type', '')),
        'phone': sanitize_text(raw_lead.get('phone', '')),
        'email': sanitize_text(raw_lead.get('email', '')),
        'source_query': sanitize_text(source_query),
        'fetched_at_iso': datetime.now().isoformat(timespec='seconds'),
        # AI Verification fields
        'ai_verified': 'YES' if raw_lead.get('ai_verified') else '',
        'ai_reason': sanitize_text(raw_lead.get('ai_reason', '')),
        'ai_confidence': ensure_int_optional(raw_lead.get('ai_confidence')),
        # AI Filter fields (post-crawl relevance)
        'ai_action': sanitize_text(raw_lead.get('ai_action', '')),
        'ai_flags': '; '.join(raw_lead.get('ai_flags', [])) if isinstance(raw_lead.get('ai_flags'), list) else sanitize_text(raw_lead.get('ai_flags', '')),
        'ai_filter_reason': sanitize_text(raw_lead.get('ai_filter_reason', '')),
        'ai_filter_confidence': ensure_int_optional(raw_lead.get('ai_filter_confidence')),
        # Keep raw heavy fields for downstream analysis (not part of CSV schema)
        'html': raw_lead.get('html', ''),
        'visible_text': raw_lead.get('visible_text', ''),
    }

    # Assign sales label based on computed fields
    sales_label, sales_reason = assign_sales_label(result)
    result['sales_label'] = sales_label
    result['sales_reason'] = sales_reason

    return result


def deduplicate_leads(leads: List[Dict]) -> List[Dict]:
    """
    Deduplicate leads by normalized URL (primary) or store_name+city (fallback).

    Rules:
    - Primary key: normalized URL
    - If URL missing but store_name exists: dedupe by (normalized_store_name + city)
    - Always keep the lead with higher score

    Args:
        leads: List of normalized lead dicts

    Returns:
        Deduplicated list of leads
    """
    # Track seen URLs and name+city combos
    url_to_lead: Dict[str, Dict] = {}
    name_city_to_lead: Dict[str, Dict] = {}

    for lead in leads:
        url = lead.get('url', '').strip()
        store_name = lead.get('store_name', '').strip()
        city = lead.get('city', '').strip()
        score = lead.get('score', 0)

        if url:
            # Primary dedup by URL
            if url in url_to_lead:
                existing = url_to_lead[url]
                if score > existing.get('score', 0):
                    url_to_lead[url] = lead
            else:
                url_to_lead[url] = lead
        elif store_name:
            # Fallback dedup by normalized name + city
            norm_name = normalize_store_name(store_name)
            norm_city = normalize_store_name(city)
            key = f"{norm_name}|{norm_city}"

            if key in name_city_to_lead:
                existing = name_city_to_lead[key]
                if score > existing.get('score', 0):
                    name_city_to_lead[key] = lead
            else:
                name_city_to_lead[key] = lead

    # Combine results
    result = list(url_to_lead.values()) + list(name_city_to_lead.values())

    logger.info(f"Deduplication: {len(leads)} -> {len(result)} leads")

    return result


def normalize_leads(raw_leads: List[Dict], source_query: str = '', region: str = '') -> List[Dict]:
    """
    Full normalization pipeline: map to schema, deduplicate, sort by score.

    Args:
        raw_leads: List of raw lead dicts from processor
        source_query: Search query that found these leads
        region: Region name

    Returns:
        List of normalized, deduplicated, sorted lead dicts
    """
    # Map to final schema
    normalized = [
        map_to_final_schema(lead, source_query=source_query, region=region)
        for lead in raw_leads
    ]

    # Deduplicate
    deduped = deduplicate_leads(normalized)

    # Sort by sales_label priority (○=2, △=1, ×=0), then lead_score desc
    _label_priority = {'○': 2, '△': 1, '×': 0}
    deduped.sort(
        key=lambda x: (
            _label_priority.get(x.get('sales_label', '×'), 0),
            x.get('lead_score', 0),
        ),
        reverse=True
    )

    return deduped


def get_schema_columns() -> List[str]:
    """Return the final schema column names in order."""
    return FINAL_SCHEMA.copy()


def get_header_labels() -> Dict[str, str]:
    """Return the Japanese header labels mapping."""
    return HEADER_LABELS.copy()
