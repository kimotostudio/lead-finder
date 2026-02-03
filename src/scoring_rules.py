"""
Manus-like scoring boost rules for lead quality assessment.
Adjusts score based on signals indicating small, individual, consultation-based practices.

Also includes solo business detection that boosts scores for solo/small practitioners
to prevent them from being filtered out by min_score filters.
"""
import re
from typing import Dict, Tuple, List
from urllib.parse import urlparse


# ============================================================
# POSITIVE SIGNALS - Increase score
# ============================================================

# Core service keywords (strong positive)
CORE_SERVICE_KEYWORDS = {
    # Counseling/therapy (highest weight)
    '相談': 15,
    '対話': 12,
    '傾聴': 15,
    'カウンセリング': 15,
    'セラピー': 12,
    'コーチング': 12,
    'メンタルサポート': 15,
    '心の整理': 12,
    '内省': 10,
    '悩み相談': 15,

    # Session types
    '個人セッション': 20,
    'プライベートセッション': 20,
    'マンツーマン': 15,
    '1対1': 12,
    '少人数': 10,
}

# Business style keywords (medium positive)
PRIVATE_STYLE_KEYWORDS = {
    '完全予約制': 20,
    '予約制': 12,
    '紹介制': 15,
    '自宅サロン': 15,
    '出張': 10,
    'プライベート': 12,
    '個人': 10,
    '小さなサロン': 12,
    'プライベートサロン': 15,
    'ひとりサロン': 15,
    '女性専用': 8,
    '完全個室': 10,
}

# Contact/booking keywords (light positive)
CONTACT_KEYWORDS = {
    'お問い合わせ': 5,
    'ご予約': 8,
    'ご相談': 10,
    '初回相談': 12,
    '無料相談': 10,
    'LINE予約': 5,
    '電話予約': 5,
}

# Menu/pricing keywords (light positive - indicates real business)
MENU_KEYWORDS = {
    'メニュー': 5,
    '料金': 5,
    '価格': 5,
    'セッション料金': 8,
    'カウンセリング料金': 8,
    '初回': 5,
    '体験': 5,
}

# ============================================================
# NEGATIVE SIGNALS - Decrease score (but don't filter)
# ============================================================

# Beauty-only services (penalty unless has counseling keywords)
BEAUTY_ONLY_KEYWORDS = {
    '脱毛': -20,
    'ネイル': -15,
    'まつげ': -15,
    'エステ': -10,  # Lighter penalty - some have counseling
    '美容院': -15,
    '美容室': -15,
    'ヘアサロン': -10,
    'フェイシャル': -10,
    '痩身': -10,
    'ダイエット': -8,
}

# School/academy style (penalty)
SCHOOL_KEYWORDS = {
    'スクール': -15,
    '講座一覧': -15,
    'カリキュラム': -12,
    '入学': -15,
    '受講料': -10,
    '資格取得': -10,
    '養成講座': -12,
    '認定講座': -10,
}

# Corporate style (penalty)
CORPORATE_KEYWORDS = {
    '法人向け': -15,
    '企業研修': -15,
    '全国展開': -20,
    '多店舗': -15,
    'フランチャイズ': -20,
}

# ============================================================
# SITE TYPE BONUSES
# ============================================================

# Site builder bonuses (indicates small business)
SITE_TYPE_BONUS = {
    'peraichi': 15,
    'jimdo': 12,
    'wix': 10,
    'crayon': 15,
    'crayonsite': 15,
    'goope': 12,
    'ownd': 10,
    'base': 8,
    'stores': 8,
    'wordpress': 5,  # Lower - can be anything
}


def _count_keyword_score(text: str, keyword_scores: Dict[str, int]) -> Tuple[int, List[str]]:
    """
    Count score from keyword matches.

    Returns:
        Tuple of (total_score, matched_keywords)
    """
    if not text:
        return 0, []

    total = 0
    matched = []
    text_lower = text.lower()

    for keyword, score in keyword_scores.items():
        if keyword.lower() in text_lower:
            total += score
            matched.append(keyword)

    return total, matched


def _has_counseling_context(text: str) -> bool:
    """Check if text has counseling/coaching context."""
    counseling_words = ['カウンセリング', 'コーチング', '相談', 'セラピー', 'メンタル', '心']
    return any(word in text for word in counseling_words)


def boost_score(lead: Dict) -> Tuple[int, List[str]]:
    """
    Calculate score boost based on lead signals.

    Args:
        lead: Lead dictionary with url, shop_name, reasons, etc.

    Returns:
        Tuple of (score_boost, reasons_list)
    """
    url = lead.get('url', '')
    name = lead.get('shop_name', '') or lead.get('name', '')
    reasons = lead.get('reasons', '')
    site_type = lead.get('site_type', '')
    visible_text = lead.get('visible_text', '')

    # Combine all text for analysis
    combined_text = f"{name} {reasons} {visible_text}"

    total_boost = 0
    boost_reasons = []

    # 1. Core service keywords (highest priority)
    score, matched = _count_keyword_score(combined_text, CORE_SERVICE_KEYWORDS)
    if score > 0:
        total_boost += min(score, 40)  # Cap at 40
        boost_reasons.extend([f'+{k}' for k in matched[:3]])  # Show top 3

    # 2. Private style keywords
    score, matched = _count_keyword_score(combined_text, PRIVATE_STYLE_KEYWORDS)
    if score > 0:
        total_boost += min(score, 30)
        boost_reasons.extend([f'+{k}' for k in matched[:2]])

    # 3. Contact keywords
    score, matched = _count_keyword_score(combined_text, CONTACT_KEYWORDS)
    if score > 0:
        total_boost += min(score, 15)

    # 4. Menu keywords
    score, matched = _count_keyword_score(combined_text, MENU_KEYWORDS)
    if score > 0:
        total_boost += min(score, 10)

    # 5. Site type bonus
    site_type_lower = site_type.lower() if site_type else ''
    for platform, bonus in SITE_TYPE_BONUS.items():
        if platform in site_type_lower or platform in url.lower():
            total_boost += bonus
            boost_reasons.append(f'+{platform}')
            break

    # 6. Beauty penalty (unless has counseling context)
    if not _has_counseling_context(combined_text):
        penalty, matched = _count_keyword_score(combined_text, BEAUTY_ONLY_KEYWORDS)
        if penalty < 0:
            total_boost += max(penalty, -30)  # Cap penalty at -30
            boost_reasons.append('△美容系')

    # 7. School penalty
    penalty, matched = _count_keyword_score(combined_text, SCHOOL_KEYWORDS)
    if penalty < 0:
        total_boost += max(penalty, -20)
        boost_reasons.append('△スクール系')

    # 8. Corporate penalty
    penalty, matched = _count_keyword_score(combined_text, CORPORATE_KEYWORDS)
    if penalty < 0:
        total_boost += max(penalty, -25)
        boost_reasons.append('△法人向け')

    # 9. Penalize low-evidence pages: no pricing AND no profile
    reasons_field = lead.get('reasons', '') or ''
    if 'no_pricing' in reasons_field and 'no_profile' in reasons_field:
        total_boost -= 35
        boost_reasons.append('penalize:no_evidence')

    # 10. Penalize missing contact info
    phone = lead.get('phone') or lead.get('tel') or ''
    email = lead.get('email') or ''
    if not phone and not email:
        total_boost -= 20
        boost_reasons.append('penalize:no_contact')

    # 11. Penalize known thin-platforms unless other strong signals exist
    platform_lower = url.lower() if url else ''
    platform_hits = ['ameblo', 'peraichi', 'fc2', 'linktr.ee', 'note.com']
    for p in platform_hits:
        if p in platform_lower or p in site_type_lower:
            # only penalize lightly if there are no core service keywords
            if not _has_counseling_context(combined_text):
                total_boost -= 40
                boost_reasons.append(f'penalize:platform_{p}')
            break

    # 12. Booking detection: explicit reservation/booking words -> extra boost
    if '予約フォーム' in combined_text or 'オンライン予約' in combined_text or 'ご予約' in combined_text:
        total_boost += 15
        boost_reasons.append('boost:booking_link')

    return total_boost, boost_reasons


def derive_comment(lead: Dict) -> str:
    """
    Generate a 1-line comment about the lead quality.

    Args:
        lead: Lead dictionary

    Returns:
        Comment string
    """
    url = lead.get('url', '')
    name = lead.get('shop_name', '') or lead.get('name', '')
    reasons = lead.get('reasons', '')
    site_type = lead.get('site_type', '')
    visible_text = lead.get('visible_text', '')

    combined_text = f"{name} {reasons} {visible_text}"

    comments = []

    # Check site type
    if site_type:
        comments.append(site_type)

    # Check for key signals
    if '完全予約制' in combined_text:
        comments.append('予約制')

    if any(kw in combined_text for kw in ['カウンセリング', 'コーチング', '相談']):
        comments.append('相談系')

    if any(kw in combined_text for kw in ['個人', 'プライベート', 'マンツーマン']):
        comments.append('個人')

    # Check for negatives
    if any(kw in combined_text for kw in ['スクール', '講座', '養成']):
        comments.append('△スクール')

    if any(kw in combined_text for kw in ['脱毛', 'ネイル', 'まつげ']):
        comments.append('△美容')

    # Check for missing booking
    if 'no_booking' in reasons:
        comments.append('no_booking')

    return ', '.join(comments[:4]) if comments else ''


def apply_scoring_boost(leads: List[Dict]) -> List[Dict]:
    """
    Apply scoring boost to all leads and update their scores.

    Args:
        leads: List of lead dictionaries

    Returns:
        Updated list with boosted scores
    """
    for lead in leads:
        original_score = int(lead.get('score', 0))
        boost, boost_reasons = boost_score(lead)

        # Apply boost (ensure 0-100 range)
        new_score = max(0, min(100, original_score + boost))
        lead['score'] = new_score

        # Update comment
        comment = derive_comment(lead)
        existing_reasons = lead.get('reasons', '')
        if comment and comment not in existing_reasons:
            lead['comment'] = comment

        # Store boost info for debugging
        if boost != 0:
            lead['score_boost'] = boost
            lead['boost_reasons'] = ', '.join(boost_reasons)

    return leads


def _extract_copyright_year(text: str) -> int:
    """Return earliest copyright year found in text, or 0 if none."""
    if not text:
        return 0
    years = []
    for match in re.findall(r'\xa9\s*(\d{4})', text):
        years.append(int(match))
    for match in re.findall(r'Copyright\s*(\d{4})', text, re.I):
        years.append(int(match))
    return min(years) if years else 0


def compute_weakness_for_lead(lead: Dict) -> Tuple[int, List[str]]:
    """
    Compute a weakness score (higher = more improvement opportunity) and reasons.

    Higher weakness score = weaker website = better lead for outreach.

    Categories:
    A. Content Analysis (text length, paragraphs)
    B. Visual Elements (images)
    C. Site Structure (navigation, footer)
    D. Contact/Booking (phone, email, forms)
    E. Pricing/Services
    F. Professionalism (about page, profile)
    G. Technical Quality (SSL, viewport)
    H. Update Frequency (copyright year)
    I. Template Detection (default text)

    Returns: (weakness_score:int, reasons:list[str])
    """
    reasons: List[str] = []
    score = 0

    # Defensive access
    url = (lead.get('url') or '')
    html = (lead.get('html') or '')
    visible = (lead.get('visible_text') or '')
    text = (lead.get('text') or '')
    title = (lead.get('shop_name') or lead.get('title') or '')
    combined = ' '.join([title, visible, text]).strip()
    combined_lower = combined.lower()
    html_lower = html.lower()

    # ============================================================
    # A. CONTENT ANALYSIS
    # ============================================================
    text_len = len(visible)
    if text_len < 300:
        score += 30
        reasons.append('コンテンツ極小(300文字未満)')
    elif text_len < 500:
        score += 20
        reasons.append('コンテンツ少(500文字未満)')
    elif text_len < 1000:
        score += 10
        reasons.append('コンテンツやや少')

    # Check for repetitive content (same phrases repeated)
    if visible:
        words = visible.split()
        if len(words) > 20:
            unique_ratio = len(set(words)) / len(words)
            if unique_ratio < 0.3:
                score += 10
                reasons.append('繰り返しコンテンツ')

    # ============================================================
    # B. VISUAL ELEMENTS
    # ============================================================
    img_count = html_lower.count('<img')
    if img_count == 0:
        score += 25
        reasons.append('画像なし')
    elif img_count < 3:
        score += 15
        reasons.append('画像3枚未満')

    # Check for stock photo indicators
    stock_indicators = ['stock', 'shutterstock', 'istock', 'getty', 'pixabay', 'unsplash', 'pexels']
    if any(ind in html_lower for ind in stock_indicators):
        score += 10
        reasons.append('ストック画像使用')

    # ============================================================
    # C. SITE STRUCTURE
    # ============================================================
    # No navigation menu
    has_nav = '<nav' in html_lower or 'メニュー' in combined or 'menu' in html_lower
    if not has_nav:
        score += 15
        reasons.append('ナビゲーションなし')

    # No footer
    if '<footer' not in html_lower:
        score += 10
        reasons.append('フッターなし')

    # Single page (no internal links)
    internal_links = html_lower.count('href="/')
    if internal_links < 3:
        score += 15
        reasons.append('ページ構成シンプル')

    # ============================================================
    # D. CONTACT / BOOKING
    # ============================================================
    # Check phone number
    phone_patterns = [r'\d{2,4}-\d{2,4}-\d{4}', r'\d{10,11}', r'tel:']
    has_phone = any(re.search(p, combined_lower) for p in phone_patterns) or bool(lead.get('phone'))
    if not has_phone:
        score += 15
        reasons.append('電話番号なし')

    # Check email
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    has_email = bool(re.search(email_pattern, combined)) or 'mailto:' in html_lower or bool(lead.get('email'))
    if not has_email:
        score += 10
        reasons.append('メールアドレスなし')

    # Check contact form
    form_keywords = ['お問い合わせフォーム', 'フォーム', 'contact form', '<form']
    has_form = any(k.lower() in html_lower or k in combined for k in form_keywords)
    if not has_form:
        score += 10
        reasons.append('問い合わせフォームなし')

    # Check booking system
    reservation_keywords = ['予約', '予約フォーム', 'ネット予約', 'オンライン予約', 'カレンダー', '予約する', '予約こちら']
    has_reservation = any(k in combined for k in reservation_keywords)
    if not has_reservation:
        score += 20
        reasons.append('予約システムなし')

    # Social media links only (no direct contact)
    social_domains = ['instagram', 'twitter', 'facebook', 'line.me', 'tiktok']
    has_social = any(s in html_lower for s in social_domains)
    if has_social and not has_phone and not has_email and not has_form:
        score += 25
        reasons.append('SNSリンクのみ')

    # ============================================================
    # E. PRICING / SERVICES
    # ============================================================
    price_keywords = ['料金', '価格', '費用', '¥', '円', 'price', 'メニュー', '料金表']
    if not any(k in combined_lower or k.lower() in combined_lower for k in price_keywords):
        score += 15
        reasons.append('料金表示なし')

    # Vague services
    vague_only = re.search(r'やってます|行っています|提供しています', combined) and text_len < 500
    if vague_only:
        score += 10
        reasons.append('サービス説明曖昧')

    # ============================================================
    # F. PROFESSIONALISM
    # ============================================================
    # No about/profile page
    profile_keywords = ['プロフィール', '経歴', '資格', '自己紹介', 'について', 'ご挨拶', 'about']
    if not any(k.lower() in combined_lower for k in profile_keywords):
        score += 20
        reasons.append('プロフィールなし')

    # No business hours
    hours_keywords = ['営業時間', '受付時間', '診療時間', 'hours', '定休日']
    if not any(k in combined_lower or k.lower() in combined_lower for k in hours_keywords) and not lead.get('business_hours'):
        score += 10
        reasons.append('営業時間なし')

    # No location/access info
    location_keywords = ['アクセス', '所在地', '住所', '地図', 'map', '最寄り駅']
    if not any(k.lower() in combined_lower for k in location_keywords):
        score += 15
        reasons.append('所在地情報なし')

    # ============================================================
    # G. TECHNICAL QUALITY
    # ============================================================
    # HTTP only (no HTTPS)
    if url.startswith('http://') and not url.startswith('https://'):
        score += 15
        reasons.append('SSL未対応')

    # Mobile unoptimized: missing viewport meta
    if html and 'name="viewport"' not in html_lower:
        score += 10
        reasons.append('モバイル非対応')

    # ============================================================
    # H. UPDATE FREQUENCY
    # ============================================================
    year = _extract_copyright_year(combined + html)
    current_year = 2026
    if year:
        years_old = current_year - year
        if years_old > 2:
            score += 30
            reasons.append(f'更新{years_old}年以上前')
        elif years_old > 1:
            score += 20
            reasons.append(f'更新{years_old}年前')
        elif years_old == 1:
            score += 10
            reasons.append('更新1年前')

    # ============================================================
    # I. TEMPLATE DETECTION
    # ============================================================
    template_keywords = ['サンプル', 'ダミー', 'lorem ipsum', 'example text', 'テンプレート']
    if any(k.lower() in combined_lower for k in template_keywords):
        score += 30
        reasons.append('テンプレート未編集')

    # Peraichi default sections
    peraichi_defaults = ['ペライチ', 'peraichi.com', 'ここにテキスト', 'ここに説明']
    if any(d in combined_lower or d in html_lower for d in peraichi_defaults):
        if 'ここにテキスト' in combined_lower or 'ここに説明' in combined_lower:
            score += 15
            reasons.append('ペライチデフォルト')

    # ============================================================
    # NEGATIVE: Too polished (subtract from weakness)
    # ============================================================
    polished_signals = ['LP', 'SEO対策', 'Googleタグマネージャー', 'googletagmanager', 'gtm.', 'ga4', 'analytics']
    if any(sig.lower() in combined_lower or sig.lower() in html_lower for sig in polished_signals):
        score -= 10
        reasons.append('polished(-)')

    # Professional design indicators
    pro_indicators = ['wordpress', 'squarespace', 'shopify']
    has_pro = any(p in html_lower for p in pro_indicators)
    if has_pro and text_len > 2000:
        score -= 15
        reasons.append('professional_site(-)')

    # Clamp
    final = max(0, min(100, int(score)))
    return final, reasons


def compute_weakness(leads: List[Dict]) -> List[Dict]:
    """Compute weakness fields for a list of leads in-place and return list."""
    for lead in leads:
        w_score, w_reasons = compute_weakness_for_lead(lead)
        lead['weakness_score'] = w_score
        lead['weakness_reasons'] = w_reasons
        # grade
        if w_score >= 50:
            lead['weakness_grade'] = 'A'
        elif w_score >= 30:
            lead['weakness_grade'] = 'B'
        else:
            lead['weakness_grade'] = 'C'

    return leads


# ============================================================
# SOLO BUSINESS SCORE BOOST
# ============================================================
# These platforms typically indicate small/solo operations

SOLO_PLATFORM_DOMAINS = {
    'peraichi.com': 40,
    'jimdofree.com': 35,
    'wixsite.com': 30,
    'strikingly.com': 30,
    'weebly.com': 25,
    'wordpress.com': 20,
    'fc2.com': 25,
    'seesaa.net': 20,
    'livedoor.blog': 15,
    'amebaownd.com': 30,
    'jimdo.com': 35,
    'shopinfo.jp': 30,
    'crayonsite.net': 35,
    'goope.jp': 30,
    'ownd.jp': 25,
    'stores.jp': 20,
    'base.shop': 20,
}

# Title keywords that strongly indicate solo/small business
SOLO_TITLE_KEYWORDS = {
    '個人': 15,
    'の部屋': 20,
    'プライベート': 15,
    'パーソナル': 15,
    '完全予約制': 25,
    '1対1': 20,
    'マンツーマン': 20,
    '自宅サロン': 30,
    '出張': 15,
    'オンライン専門': 20,
    '女性専用': 15,
    'ひとりサロン': 30,
    'プライベートサロン': 25,
}

# Text content keywords indicating solo/small business
SOLO_TEXT_KEYWORDS = {
    '一人で運営': 30,
    '個人事業主': 25,
    'ひとりサロン': 30,
    '自宅の一室': 30,
    '少人数制': 15,
    '完全個別対応': 25,
    '個人でやっています': 30,
    '主宰': 15,
    'オーナー兼': 20,
}


def apply_solo_score_boost(lead: Dict) -> Tuple[int, List[str]]:
    """
    Calculate additional score boost for solo/small businesses.

    This boost helps solo practitioners with weak websites avoid being filtered
    out by min_score filters. The boost is applied based on:
    1. Platform/domain indicators (peraichi, jimdo, etc.)
    2. Title keywords (個人, プライベート, etc.)
    3. Text content keywords (一人で運営, etc.)
    4. Existing solo_classification from SoloClassifier

    Args:
        lead: Lead dictionary with url, shop_name, visible_text, solo_classification, etc.

    Returns:
        Tuple of (boost_amount, reasons_list)
    """
    url = lead.get('url', '').lower()
    name = lead.get('shop_name', '') or lead.get('name', '') or ''
    title = lead.get('title', '') or name
    visible_text = lead.get('visible_text', '') or ''
    solo_classification = lead.get('solo_classification', 'unknown')
    solo_score_val = lead.get('solo_score')

    boost = 0
    reasons = []

    # 1. Boost based on existing solo_classification (from SoloClassifier)
    if solo_classification == 'solo':
        boost += 20
        reasons.append('solo_classified:+20')
    elif solo_classification == 'small':
        boost += 10
        reasons.append('small_classified:+10')
    elif solo_classification == 'corporate':
        boost -= 10
        reasons.append('corporate_classified:-10')

    # Additional boost for high solo_score
    if solo_classification != 'corporate' and solo_score_val is not None:
        try:
            solo_score_int = int(solo_score_val)
            if solo_score_int >= 10:
                boost += 15
                reasons.append(f'high_solo_score({solo_score_int}):+15')
            elif solo_score_int >= 5:
                boost += 10
                reasons.append(f'medium_solo_score({solo_score_int}):+10')
        except (ValueError, TypeError):
            pass

    # 2. Platform/domain boost
    domain = urlparse(url).netloc.lower() if url else ''
    for platform, platform_boost in SOLO_PLATFORM_DOMAINS.items():
        if platform in domain:
            boost += platform_boost
            reasons.append(f'platform_{platform}:+{platform_boost}')
            break

    # 3. Title keyword boost
    title_lower = title.lower()
    title_boost = 0
    for keyword, kw_boost in SOLO_TITLE_KEYWORDS.items():
        if keyword.lower() in title_lower:
            title_boost = max(title_boost, kw_boost)  # Take highest match
    if title_boost > 0:
        boost += title_boost
        reasons.append(f'title_solo_kw:+{title_boost}')

    # 4. Text content keyword boost
    text_lower = visible_text.lower()
    text_boost = 0
    matched_text_kw = None
    for keyword, kw_boost in SOLO_TEXT_KEYWORDS.items():
        if keyword.lower() in text_lower:
            if kw_boost > text_boost:
                text_boost = kw_boost
                matched_text_kw = keyword
    if text_boost > 0:
        boost += text_boost
        reasons.append(f'text_solo_kw({matched_text_kw}):+{text_boost}')

    # 5. Check for absence of corporate info (bonus for lacking corp signals)
    corp_signals = ['会社概要', '法人情報', '株式会社', '有限会社', '従業員数', '資本金']
    has_corp = any(term in visible_text for term in corp_signals)
    if not has_corp and len(visible_text) > 200:
        boost += 10
        reasons.append('no_corp_info:+10')

    # Cap total adjustment to keep score stable.
    boost = max(-30, min(80, boost))

    return boost, reasons


def apply_solo_boost_to_leads(leads: List[Dict]) -> List[Dict]:
    """
    Apply solo-based score boost to all leads.

    This should be called BEFORE min_score filtering to ensure
    solo/small practitioners aren't filtered out due to weak websites.

    Args:
        leads: List of lead dictionaries

    Returns:
        Updated list with solo-boosted scores
    """
    for lead in leads:
        solo_boost, solo_boost_reasons = apply_solo_score_boost(lead)

        if solo_boost != 0:
            original_score = int(lead.get('score', 0))
            new_score = max(0, min(100, original_score + solo_boost))
            lead['score'] = new_score

            # Add solo boost info to existing boost_reasons
            existing_boost_reasons = lead.get('boost_reasons', '')
            if isinstance(existing_boost_reasons, list):
                existing_boost_reasons = ', '.join(existing_boost_reasons)

            solo_reasons_str = ', '.join(solo_boost_reasons)
            if existing_boost_reasons:
                lead['boost_reasons'] = f"{existing_boost_reasons}; SOLO: {solo_reasons_str}"
            else:
                lead['boost_reasons'] = f"SOLO: {solo_reasons_str}"

            # Track solo boost separately for debugging
            lead['solo_boost'] = solo_boost
            lead['solo_boost_reasons'] = solo_boost_reasons

    return leads
