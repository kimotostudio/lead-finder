"""
Solo proprietor likelihood scoring and lightweight URL inspection.
"""
import ipaddress
import logging
import random
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from config.settings import MAX_REDIRECTS, TIMEOUT, USER_AGENTS
from src.utils.retry import exponential_backoff_retry

logger = logging.getLogger(__name__)


CORP_TERMS = [
    '株式会社', '有限会社', '合同会社', '合資会社', '合名会社', '一般社団法人', '一般財団法人',
    'NPO法人', '医療法人', '学校法人', '宗教法人', '社会福祉法人', '協同組合',
    '弁護士法人', '税理士法人', '行政書士法人',
    'inc', 'llc', 'ltd', 'limited', 'corporation', 'corp',
]
EXEC_TITLES = [
    '代表取締役', '取締役', '執行役員', 'CEO', 'CFO', 'COO', 'CTO', '取締役会',
]

SOLO_SIGNALS = [
    ('個人サロン', 4),
    ('自宅サロン', 4),
    ('自宅の一室', 4),
    ('自宅', 4),
    ('ホームサロン', 4),
    ('プライベートサロン', 4),
    ('隠れ家サロン', 4),
    ('自宅開業', 4),
    ('出張専門', 4),
    ('オーナーセラピスト', 4),
    ('代表兼施術者', 4),
    ('完全予約制', 3),
    ('予約制', 3),
    ('完全予約', 3),
    ('個人', 3),
    ('ひとり', 3),
    ('一人', 3),
    ('1人', 3),
    ('1対1', 3),
    ('マンツーマン', 3),
    ('女性専用', 3),
    ('主宰', 2),
    ('主催者', 2),
    ('オーナー', 2),
    ('店主', 2),
    ('一人で', 2),
    ('私が', 2),
    ('私の', 2),
    ('わたしの', 2),
    ('施術者', 2),
    ('セラピスト兼', 2),
    ('施術歴', 2),
    ('経歴', 2),
    ('セラピスト', 2),
    ('カウンセラー', 2),
    ('占い師', 2),
    ('講師', 2),
    ('代表', 2),
    ('運営者情報', 2),
    ('運営責任者', 2),
]

SMALL_SIGNALS = [
    ('サロン', 1),
    ('スタジオ', 1),
    ('ルーム', 1),
    ('アトリエ', 1),
    ('アットホーム', 1),
    ('丁寧', 1),
    ('心を込めて', 1),
    ('寄り添う', 1),
    ('あなたに合わせた', 1),
]

PROFILE_SIGNALS = [
    'プロフィール',
    '自己紹介',
    'ご挨拶',
    'ごあいさつ',
    '私について',
]

CONTACT_SIGNALS = [
    'お問い合わせ',
    '問い合わせ',
    '連絡先',
    '予約',
    'フォーム',
]

SOFT_CORP_SIGNS = [
    ('会社概要', -2),
    ('企業情報', -2),
    ('沿革', -2),
    ('理念', -2),
    ('ミッション', -2),
    ('ビジョン', -2),
]

RECRUIT_SIGNS = [
    '採用', '求人', 'リクルート',
]

BRANCH_SIGNS = [
    '支店', '店舗一覧', '拠点',
]

SOLO_PLATFORM_DOMAINS = [
    'ameblo.jp',
    'amebaownd.com',
    'note.com',
    'lit.link',
    'peraichi.com',
    'studio.site',
    'jimdofree.com',
]

ABOUT_PATTERNS = [
    'about', 'company', 'profile', 'concept', 'staff', 'outline', 'info', 'greeting',
    'プロフィール', 'コンセプト', '自己紹介', '会社概要', '運営者情報', 'スタッフ',
]
CONTACT_PATTERNS = [
    'contact', 'inquiry', 'form', 'reservation', 'access',
    'お問い合わせ', '問い合わせ', '予約', 'アクセス', '連絡先',
]
LEGAL_PATTERNS = [
    'tokutei', 'tokusho', 'law', 'commerce',
    '特定商取引法', '特商法', '運営者情報', '運営責任者', '会社概要',
]

BLOCK_PAGE_KEYWORDS = [
    'captcha', 'cloudflare', 'access denied', 'forbidden', 'bot',
]


@dataclass
class FetchResult:
    ok: bool
    url_status: str
    error_code: str
    html: str = ''
    final_url: str = ''
    content_type: str = ''
    status_code: int = 0
    normalized_url: str = ''


def _random_user_agent() -> str:
    return random.choice(USER_AGENTS)


def _normalize_origin_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    if not url:
        return None, 'INVALID_URL'
    url = url.strip()
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https'):
        return None, 'INVALID_SCHEME'
    host = parsed.hostname
    if not host:
        return None, 'MISSING_HOST'

    try:
        ipaddress.ip_address(host)
        return None, 'IP_HOST'
    except ValueError:
        pass

    host_lower = host.lower()
    if host_lower in ('localhost', '127.0.0.1') or host_lower.endswith(('.local', '.internal', '.lan')):
        return None, 'NON_PUBLIC_HOST'
    if '.' not in host_lower:
        return None, 'INVALID_TLD'

    port = f":{parsed.port}" if parsed.port else ''
    return f"https://{host_lower}{port}", None


def _is_blocked_response(status_code: int, body_text: str) -> bool:
    if status_code in (403, 429):
        return True
    if status_code in (401, 503):
        lower = body_text.lower()
        return any(k in lower for k in BLOCK_PAGE_KEYWORDS)
    return False


def analyze_fetch_response(status_code: int, content_type: str, body_text: str) -> Tuple[bool, str, str]:
    """
    Return (ok, url_status, error_code) based on response.
    """
    content_type = (content_type or '').lower()
    if status_code >= 400:
        if _is_blocked_response(status_code, body_text or ''):
            return False, 'BLOCKED', f'BLOCKED_{status_code}'
        if status_code == 404:
            return False, 'INVALID', 'HTTP_404'
        if 400 <= status_code < 500:
            return False, 'INVALID', f'HTTP_{status_code}'
        return False, 'INVALID', f'HTTP_{status_code}'
    if content_type and 'text/html' not in content_type:
        return False, 'INVALID', 'NON_HTML'
    return True, 'OK', 'OK'


@exponential_backoff_retry(max_retries=2, base_delay=1.0)
def _fetch_response(session: requests.Session, url: str) -> requests.Response:
    headers = {
        'User-Agent': _random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
    }
    return session.get(
        url,
        headers=headers,
        timeout=TIMEOUT,
        allow_redirects=True,
        verify=False
    )


def _extract_text_blocks(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, 'html.parser')
    title = soup.title.string.strip()[:200] if soup.title and soup.title.string else ''
    h1 = soup.find('h1')
    h1_text = h1.get_text(strip=True)[:200] if h1 else ''

    main = soup.find('main') or soup.body or soup
    main_text = main.get_text(separator=' ', strip=True)
    main_text = ' '.join(main_text.split())[:3000]

    footer = soup.find('footer')
    footer_text = ''
    if footer:
        footer_text = footer.get_text(separator=' ', strip=True)
        footer_text = ' '.join(footer_text.split())[:1000]

    combined = ' '.join([title, h1_text, main_text, footer_text]).strip()
    return {
        'title': title,
        'h1': h1_text,
        'main': main_text,
        'footer': footer_text,
        'combined': combined
    }


def _extract_snippet(text: str, term: str, window: int = 18) -> str:
    if not text or not term:
        return ''
    idx = text.find(term)
    if idx == -1:
        return ''
    start = max(0, idx - window)
    end = min(len(text), idx + len(term) + window)
    snippet = text[start:end].strip()
    return re.sub(r'\s+', ' ', snippet)


def _find_first_link(soup: BeautifulSoup, base_url: str, patterns: List[str]) -> Optional[str]:
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '').strip()
        if not href or href.startswith(('mailto:', 'tel:', 'javascript:')):
            continue
        text = a_tag.get_text(strip=True)
        target = f"{href} {text}".lower()
        if any(p.lower() in target for p in patterns):
            abs_url = urljoin(base_url, href)
            if _is_same_host(base_url, abs_url):
                return abs_url
    return None


def _is_same_host(base_url: str, target_url: str) -> bool:
    try:
        return urlparse(base_url).hostname == urlparse(target_url).hostname
    except Exception:
        return False


def _staff_count_score(text: str) -> Tuple[int, Optional[int]]:
    match = re.search(r'スタッフ[^0-9]{0,6}([0-9]{1,2})\s*名', text)
    if match:
        count = int(match.group(1))
        if 2 <= count <= 4:
            return 1, count
        if count >= 5:
            return -4, count
    return 0, None


class SoloClassifier:
    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.session.max_redirects = MAX_REDIRECTS

    def inspect_home(self, url: str) -> FetchResult:
        normalized_url, error = _normalize_origin_url(url)
        if error:
            return FetchResult(
                ok=False,
                url_status='INVALID',
                error_code=error,
                normalized_url=normalized_url or ''
            )

        try:
            response = _fetch_response(self.session, normalized_url)
        except requests.exceptions.Timeout:
            return FetchResult(False, 'INVALID', 'TIMEOUT', normalized_url=normalized_url)
        except requests.exceptions.TooManyRedirects:
            return FetchResult(False, 'INVALID', 'TOO_MANY_REDIRECTS', normalized_url=normalized_url)
        except requests.exceptions.ConnectionError:
            return FetchResult(False, 'INVALID', 'DNS_FAIL', normalized_url=normalized_url)
        except Exception as e:
            logger.error(f"Fetch error for {normalized_url}: {e}")
            return FetchResult(False, 'INVALID', 'FETCH_ERROR', normalized_url=normalized_url)

        body_text = ''
        try:
            body_text = response.text or ''
        except Exception:
            body_text = ''

        ok, url_status, error_code = analyze_fetch_response(
            response.status_code,
            response.headers.get('Content-Type', ''),
            body_text
        )
        if not ok:
            return FetchResult(
                ok=False,
                url_status=url_status,
                error_code=error_code,
                normalized_url=normalized_url,
                final_url=str(response.url),
                content_type=response.headers.get('Content-Type', ''),
                status_code=response.status_code,
            )

        return FetchResult(
            ok=True,
            url_status='OK',
            error_code='OK',
            html=body_text,
            final_url=str(response.url),
            content_type=response.headers.get('Content-Type', ''),
            status_code=response.status_code,
            normalized_url=normalized_url
        )

    def classify(self, base_url: str, home_html: str) -> Dict:
        pages = []
        pages.append({'url': base_url, 'html': home_html})

        soup = BeautifulSoup(home_html, 'html.parser')
        about_url = _find_first_link(soup, base_url, ABOUT_PATTERNS)
        contact_url = _find_first_link(soup, base_url, CONTACT_PATTERNS)
        legal_url = _find_first_link(soup, base_url, LEGAL_PATTERNS)

        for candidate in [about_url, contact_url, legal_url]:
            if not candidate or len(pages) >= 4:
                continue
            if any(p['url'] == candidate for p in pages):
                continue
            try:
                resp = _fetch_response(self.session, candidate)
            except Exception:
                continue
            ok, _, _ = analyze_fetch_response(
                resp.status_code,
                resp.headers.get('Content-Type', ''),
                resp.text or ''
            )
            if not ok:
                continue
            pages.append({'url': candidate, 'html': resp.text or ''})

        combined_texts = []
        for page in pages:
            blocks = _extract_text_blocks(page['html'])
            page['text_blocks'] = blocks
            combined_texts.append(blocks['combined'])

        combined = ' '.join(combined_texts)
        combined_lower = combined.lower()

        detected_corp_terms = []
        for term in CORP_TERMS:
            if term.lower() in combined_lower:
                detected_corp_terms.append(term)
        for term in EXEC_TITLES:
            if term.lower() in combined_lower:
                detected_corp_terms.append(term)
        if re.search(r'資本金\s*\d', combined):
            detected_corp_terms.append('資本金')
        if re.search(r'従業員数\s*\d', combined):
            detected_corp_terms.append('従業員数')

        reasons = []
        snippets = []

        if detected_corp_terms:
            reasons.append('-999:corporate_terms')
            for term in detected_corp_terms:
                snippet = _extract_snippet(combined, term)
                if snippet:
                    snippets.append(snippet)
            return {
                'solo_score': -999,
                'classification': 'corporate',
                'reasons': reasons,
                'evidence_snippets': snippets[:5],
                'detected_corp_terms': list(dict.fromkeys(detected_corp_terms)),
            }

        score = 0
        base_domain = urlparse(base_url).netloc.lower().replace('www.', '') if base_url else ''

        for term, points in SOLO_SIGNALS:
            if term == '代表' and '代表取締役' in combined:
                continue
            if term == '個人' and '個人情報' in combined:
                continue
            if term in combined:
                score += points
                reasons.append(f"{points:+d}:{term}")
                snippet = _extract_snippet(combined, term)
                if snippet:
                    snippets.append(snippet)

        for platform in SOLO_PLATFORM_DOMAINS:
            if base_domain == platform or base_domain.endswith('.' + platform):
                score += 2
                reasons.append(f"+2:platform:{platform}")
                break

        if re.search(r'屋号\s*[:：]', combined):
            score += 3
            reasons.append('+3:屋号')
            snippet = _extract_snippet(combined, '屋号')
            if snippet:
                snippets.append(snippet)

        if '特定商取引法' in combined or '特商法' in combined or any(
            'tokutei' in p['url'] or 'tokusho' in p['url'] for p in pages
        ):
            score += 1
            reasons.append('+1:特商法')
            snippet = _extract_snippet(combined, '特定商取引法') or _extract_snippet(combined, '特商法')
            if snippet:
                snippets.append(snippet)

        staff_delta, staff_count = _staff_count_score(combined)
        if staff_delta != 0:
            score += staff_delta
            label = f"スタッフ{staff_count}名" if staff_count else 'スタッフ数'
            reasons.append(f"{staff_delta:+d}:{label}")

        for term, points in SMALL_SIGNALS:
            if term in combined:
                score += points
                reasons.append(f"{points:+d}:{term}")
                snippet = _extract_snippet(combined, term)
                if snippet:
                    snippets.append(snippet)
                break

        if any(term in combined for term in PROFILE_SIGNALS):
            score += 1
            reasons.append('+1:プロフィール')
            snippet = _extract_snippet(combined, 'プロフィール')
            if snippet:
                snippets.append(snippet)

        if any(term in combined for term in CONTACT_SIGNALS):
            score += 1
            reasons.append('+1:お問い合わせ')
            snippet = _extract_snippet(combined, 'お問い合わせ')
            if snippet:
                snippets.append(snippet)

        for term, points in SOFT_CORP_SIGNS:
            if term in combined:
                score += points
                reasons.append(f"{points:+d}:{term}")
                snippet = _extract_snippet(combined, term)
                if snippet:
                    snippets.append(snippet)

        for term in RECRUIT_SIGNS:
            if term in combined:
                score -= 6
                reasons.append('-6:採用')
                snippet = _extract_snippet(combined, term)
                if snippet:
                    snippets.append(snippet)
                break

        for term in BRANCH_SIGNS:
            if term in combined:
                score -= 4
                reasons.append('-4:拠点')
                snippet = _extract_snippet(combined, term)
                if snippet:
                    snippets.append(snippet)
                break

        if 'ビル' in combined or re.search(r'\d+階', combined):
            score -= 2
            reasons.append('-2:ビル/階')
            snippet = _extract_snippet(combined, 'ビル')
            if snippet:
                snippets.append(snippet)

        if score >= 8:
            classification = 'solo'
        elif score >= 4:
            classification = 'small'
        elif score <= -3:
            classification = 'corporate'
        else:
            classification = 'unknown'

        return {
            'solo_score': int(score),
            'classification': classification,
            'reasons': reasons,
            'evidence_snippets': snippets[:5],
            'detected_corp_terms': [],
        }
