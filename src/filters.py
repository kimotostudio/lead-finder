"""
Hard exclusion filter for lead relevance.
Filters out irrelevant leads BEFORE scoring to improve precision.
"""
import re
from typing import Dict, Tuple
from urllib.parse import urlparse

from src.content_analyzer import has_japanese_content


# ============================================================
# HARD EXCLUSION DOMAINS - Always filtered out
# ============================================================
EXCLUDED_DOMAINS = {
    # Ameblo platforms (cannot send cold emails)
    'ameblo.jp',
    'blog.ameba.jp',
    'ameba.jp',
    's.ameblo.jp',
    'profile.ameba.jp',

    # Blog/note platforms (cannot contact directly)
    'note.com',
    'note.mu',

    # Blog platforms (blog-only, cannot contact)
    'hatena.ne.jp',
    'hatenablog.com',
    'hatenablog.jp',
    'livedoor.blog',
    'livedoor.jp',
    'blogspot.com',
    'muragon.com',
    'goo.ne.jp',

    # Portal / aggregator sites
    'hotpepper.jp',
    'hotpepperbeauty.jp',
    'beauty.hotpepper.jp',
    'epark.jp',
    'ekiten.jp',
    'rakuten.co.jp',
    'rakuten.ne.jp',
    'tabelog.com',
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

    # SNS platforms (no direct contact)
    'instagram.com',
    'twitter.com',
    'x.com',
    'facebook.com',
    'tiktok.com',
    'youtube.com',
    'line.me',
    'linkedin.com',
    'pinterest.com',
    'lit.link',
    'linktr.ee',
    'b-spot.tv',
    'byoinnavi.jp',
    'kenkou-job.com',

    # NOTE: Solo-friendly platforms (peraichi, wix, jimdo, fc2, seesaa)
    # are NOT excluded - they get solo boost instead

    # Job / recruiting sites
    'indeed.com',
    'rikunabi.com',
    'mynavi.jp',
    'doda.jp',
    'en-japan.com',
    'careerjet.jp',
    'stanby.co.jp',
    'jobota.net',

    # Large corporate / government
    'mhlw.go.jp',
    'city.tokyo.lg.jp',
    'metro.tokyo.lg.jp',
    'pref.kanagawa.jp',
    'wikipedia.org',
}

# Explicit additional map/shortlink domains to block or inspect
MAP_DOMAINS = {
    'google.com',
    'goo.gl',
}

# ============================================================
# HARD EXCLUSION KEYWORDS - Filter if found in URL/title/text
# ============================================================

# Job/recruiting keywords
JOB_KEYWORDS = [
    '求人', '採用', '募集', 'recruit', 'career', '転職', '就職',
    'スタッフ募集', 'アルバイト', 'パート募集', '正社員',
]

# Medical institution keywords
MEDICAL_KEYWORDS = [
    '病院', 'クリニック', '心療内科', '精神科', '診療', '医療',
    '医院', '歯科', '眼科', '皮膚科', '内科', '外科',
    '小児科', '産婦人科', '整形外科', '耳鼻', '泌尿器',
    'hospital', 'clinic', 'medical',
]

# Corporate / franchise keywords
CORPORATE_KEYWORDS = [
    '株式会社', '(株)', '有限会社', '合同会社', 'LLC',
    '採用情報', '店舗一覧', 'フランチャイズ', '全国展開',
    '法人向け', '企業研修', 'BtoB', 'B2B',
    '多店舗', '全国チェーン', 'グループ会社',
]

# Aggregator / portal keywords
AGGREGATOR_KEYWORDS = [
    'おすすめ', 'ランキング', '比較', '一覧', 'まとめ',
    '口コミ', 'レビュー', '評価', '人気', 'TOP',
    '選', 'BEST', '厳選', '徹底比較', '完全ガイド',
]

# Additional explicit keyword blocks (title/snippet)
KEYWORD_BLOCKLIST = [
    'ホットペッパー',
    '楽天ビューティ',
    '食べログ',
    'EPARK',
    'Indeed',
    'リンクツリー',
    'linktr.ee',
    '求人',
    '医療法人',
    # Note: ペライチ and amebaownd removed - peraichi is solo-friendly,
    # amebaownd is blocked by domain filter
]

# ============================================================
# POSITIVE KEYWORDS - Signals of good leads
# ============================================================
GOOD_LEAD_KEYWORDS = [
    # Counseling/therapy keywords
    '相談', '対話', '傾聴', 'カウンセリング', 'セラピー',
    'コーチング', 'メンタル', '心の', '悩み',

    # Individual/private keywords
    '個人', 'プライベート', 'マンツーマン', '少人数',
    '完全予約制', '紹介制', '自宅', '出張',
    '個人セッション', 'プライベートセッション',

    # Session/service keywords
    'セッション', 'カウンセラー', 'セラピスト', 'コーチ',
]


def _extract_domain(url: str) -> str:
    """Extract domain from URL, removing www prefix."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        domain = domain.replace('www.', '')
        return domain
    except Exception:
        return ''


def _check_keywords(text: str, keywords: list) -> bool:
    """Check if any keyword is in text (case-insensitive for ASCII)."""
    if not text:
        return False
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


def is_excluded_domain(url: str) -> Tuple[bool, str]:
    """
    Check if URL's domain is in hard exclusion list.

    Returns:
        Tuple of (is_excluded, reason)
    """
    domain = _extract_domain(url)
    if not domain:
        return False, ''

    # Check exact match
    if domain in EXCLUDED_DOMAINS:
        return True, f'excluded_domain:{domain}'

    # Check subdomain match (e.g., blog.example.com -> example.com)
    for excluded in EXCLUDED_DOMAINS:
        if domain.endswith('.' + excluded) or domain == excluded:
            return True, f'excluded_domain:{excluded}'

    # Block google maps and known short map links
    try:
        parsed = urlparse(url)
        path = (parsed.path or '').lower()
        # google.com/maps and variants
        if domain == 'google.com' and path.startswith('/maps'):
            return True, 'excluded_domain:google_maps'
        # goo.gl short map links
        if domain == 'goo.gl' and path.startswith('/maps'):
            return True, 'excluded_domain:goo_gl_maps'
    except Exception:
        pass

    return False, ''


def is_job_page(url: str, title: str, text: str) -> Tuple[bool, str]:
    """Check if page is a job/recruiting page."""
    combined = f"{url} {title} {text}"
    if _check_keywords(combined, JOB_KEYWORDS):
        return True, 'job_recruiting_page'
    return False, ''


def is_keyword_blocked(url: str, title: str, text: str) -> Tuple[bool, str]:
    """Block pages that explicitly reference major portals/aggregators in title/snippet."""
    combined = f"{url} {title} {text}"
    for kw in KEYWORD_BLOCKLIST:
        if kw.lower() in combined.lower():
            # normalize reason token
            reason = kw.replace(' ', '_')
            return True, f'blocked_keyword:{reason}'
    return False, ''


def is_medical_institution(url: str, title: str, text: str) -> Tuple[bool, str]:
    """Check if page is a medical institution."""
    combined = f"{url} {title} {text}"
    if _check_keywords(combined, MEDICAL_KEYWORDS):
        # Allow if also has counseling/coaching keywords (non-medical therapy)
        if _check_keywords(combined, GOOD_LEAD_KEYWORDS):
            return False, ''
        return True, 'medical_institution'
    return False, ''


def is_corporate_site(url: str, title: str, text: str) -> Tuple[bool, str]:
    """Check if page is a corporate/franchise site.

    Only checks URL and title (NOT full text) to avoid false positives
    where a small business page merely references a corporation.
    """
    combined = f"{url} {title}"
    if _check_keywords(combined, CORPORATE_KEYWORDS):
        return True, 'corporate_franchise'
    return False, ''


def is_aggregator_page(url: str, title: str, text: str) -> Tuple[bool, str]:
    """Check if page is an aggregator/listing page."""
    combined = f"{url} {title}"

    # Check for aggregator keywords
    if _check_keywords(combined, AGGREGATOR_KEYWORDS):
        return True, 'aggregator_portal'

    # Check for list pattern (e.g., "おすすめ20選")
    if re.search(r'[0-9]{1,3}選', combined):
        return True, 'aggregator_list'

    return False, ''


def is_relevant_lead(lead: Dict) -> bool:
    """
    Check if lead passes all hard exclusion filters.

    Args:
        lead: Lead dictionary with url, title, visible_text, etc.

    Returns:
        True if lead should be kept, False if excluded
    """
    _, reason = get_filter_reason(lead)
    return reason == ''


def get_filter_reason(lead: Dict) -> Tuple[bool, str]:
    """
    Get filter result and reason for a lead.

    Args:
        lead: Lead dictionary

    Returns:
        Tuple of (is_filtered, reason)
        reason is empty string if lead passes
    """
    url = lead.get('url', '')
    title = lead.get('shop_name', '') or lead.get('title', '')
    text = lead.get('visible_text', '') or lead.get('reasons', '')

    # 1. Check domain exclusion (highest priority — portals, SNS, job sites)
    excluded, reason = is_excluded_domain(url)
    if excluded:
        return True, reason

    # 2. Check explicit keyword blocks (portal names etc.)
    excluded, reason = is_keyword_blocked(url, title, text)
    if excluded:
        return True, reason

    # 3. Check job/recruiting (title/URL only — not full text)
    excluded, reason = is_job_page(url, title, '')
    if excluded:
        return True, reason

    # 4. Check aggregator (title/URL only)
    excluded, reason = is_aggregator_page(url, title, text)
    if excluded:
        return True, reason

    # 5. Check medical institution
    excluded, reason = is_medical_institution(url, title, text)
    if excluded:
        return True, reason

    # 6. Check corporate/franchise (title/URL only)
    excluded, reason = is_corporate_site(url, title, text)
    if excluded:
        return True, reason

    return False, ''


def filter_leads(leads: list) -> Tuple[list, list]:
    """
    Filter a list of leads, returning kept and filtered leads.

    Args:
        leads: List of lead dictionaries

    Returns:
        Tuple of (kept_leads, filtered_leads)
    """
    kept = []
    filtered = []

    for lead in leads:
        is_filtered, reason = get_filter_reason(lead)
        if is_filtered:
            lead['filter_reason'] = reason
            filtered.append(lead)
        else:
            kept.append(lead)

    return kept, filtered
