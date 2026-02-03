"""
AI-powered website verification using GPT-4o-mini.

Provides:
1. Weakness verification - confirms top-scoring leads are truly "weak sites"
2. Relevance gate - filters out foreign/unrelated/portal/SNS sites before expensive crawling
"""
import os
import json
import logging
from typing import Tuple, List, Dict, Optional
from urllib.parse import urlparse
import re

logger = logging.getLogger(__name__)


FOREIGN_TLD_HINTS = (
    '.de', '.fr', '.it', '.es', '.pt', '.nl', '.ru', '.pl', '.se',
    '.no', '.dk', '.fi', '.ch', '.at', '.be', '.ie', '.cz', '.hu',
    '.ro', '.bg', '.hr', '.sk', '.si', '.lt', '.lv', '.ee',
    '.br', '.mx', '.ar', '.cl', '.co', '.pe', '.ve',
    '.cn', '.tw', '.kr', '.th', '.vn', '.ph', '.sg', '.my', '.id',
    '.za', '.ng', '.ke', '.eg', '.au', '.nz', '.uk',
)

PORTAL_SNS_DOMAIN_HINTS = (
    'instagram.com', 'twitter.com', 'x.com', 'facebook.com', 'tiktok.com', 'youtube.com',
    'line.me', 'linkedin.com', 'pinterest.com', 'lit.link', 'linktr.ee',
    'hotpepper.jp', 'hotpepperbeauty.jp', 'tabelog.com', 'epark.jp', 'ekiten.jp',
    'rakuten.co.jp', 'beauty.rakuten.co.jp', 'gnavi.co.jp', 'gurunavi.com',
    'indeed.com', 'indeed.jp', 'rikunabi.com', 'mynavi.jp', 'doda.jp', 'en-japan.com',
)


def _extract_json_object(text: str) -> Optional[Dict]:
    """Best-effort JSON extraction from model output."""
    if not text:
        return None
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        cleaned = "\n".join(lines[1:])
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()
    try:
        return json.loads(cleaned)
    except Exception:
        pass

    m = re.search(r"\{[\s\S]*\}", cleaned)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def _contains_japanese(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r'[\u3040-\u30ff\u3400-\u9fff]', text))


def _has_local_service_signal(text: str, target_btypes: List[str], target_location: str) -> bool:
    hay = f"{text or ''} {' '.join(target_btypes or [])} {target_location or ''}"
    keywords = [
        '予約', 'お問い合わせ', 'アクセス', '営業時間', 'サロン', '整体', 'カウンセリング',
        'セラピー', 'エステ', 'ヨガ', '占い', '自宅サロン', '個人', 'プライベート',
    ]
    return any(k in hay for k in keywords)


def _rule_based_relevance(url: str, title: str, snippet: str, target_btypes: List[str], target_location: str) -> Optional[Dict]:
    """
    Fast deterministic guardrail before AI call.
    Returns classification dict or None to continue with AI.
    """
    u = (url or '').lower()
    parsed = urlparse(u)
    domain = (parsed.netloc or '').replace('www.', '')
    combined = f"{title or ''} {snippet or ''}"
    combined_lower = combined.lower()

    if not domain:
        return {"keep": True, "reason": "domain不明のため保持", "category": "other", "confidence": 1}

    # Hard block obvious portal/SNS/job domains.
    for d in PORTAL_SNS_DOMAIN_HINTS:
        if domain == d or domain.endswith('.' + d):
            cat = "SNS" if d in ('instagram.com', 'twitter.com', 'x.com', 'facebook.com', 'tiktok.com', 'youtube.com', 'line.me', 'linkedin.com', 'pinterest.com', 'lit.link', 'linktr.ee') else "PORTAL"
            if d in ('indeed.com', 'indeed.jp', 'rikunabi.com', 'mynavi.jp', 'doda.jp', 'en-japan.com'):
                cat = "JOB_LISTING"
            return {"keep": False, "reason": f"rule:{d}", "category": cat.lower(), "confidence": 10}

    # Foreign TLD heuristic: drop only when JP/local signals are absent.
    looks_foreign = any(domain.endswith(tld) for tld in FOREIGN_TLD_HINTS)
    jp_signal = (
        domain.endswith('.jp')
        or 'japan' in domain
        or _contains_japanese(combined)
        or _has_local_service_signal(combined, target_btypes, target_location)
    )
    if looks_foreign and not jp_signal:
        return {"keep": False, "reason": "rule:foreign_tld", "category": "foreign", "confidence": 9}

    # If title/snippet clearly matches target business + JP signal, keep.
    btype_hit = any((b or '').lower() in combined_lower for b in (target_btypes or []))
    location_hit = (target_location or '') in combined
    if btype_hit and (location_hit or _contains_japanese(combined)):
        return {"keep": True, "reason": "rule:target_match", "category": "ok", "confidence": 9}

    return None

# Try to import openai, gracefully handle if not installed
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning("openai package not installed. AI verification disabled.")


class AIVerifier:
    """
    AI-powered verifier using GPT-4o-mini to confirm weak sites.

    Analyzes top-scoring leads to verify they are truly weak/outdated
    websites that would benefit from improvement services.
    """

    def __init__(self, api_key: str = None):
        """
        Initialize the AI verifier.

        Args:
            api_key: OpenAI API key. If not provided, reads from OPENAI_API_KEY env var.

        Raises:
            ValueError: If no API key is available.
            ImportError: If openai package is not installed.
        """
        if not OPENAI_AVAILABLE:
            raise ImportError("openai package is required. Install with: pip install openai")

        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key required. Set OPENAI_API_KEY env var or pass api_key parameter.")

        self.client = openai.OpenAI(api_key=self.api_key)
        logger.info("AIVerifier initialized with GPT-4o-mini")

    def verify_weak_site(
        self,
        url: str,
        html_snippet: str,
        title: str,
        weakness_score: int = 0,
        weakness_reasons: List[str] = None
    ) -> Tuple[bool, str, int]:
        """
        Verify if a website is truly weak/outdated using AI.

        Args:
            url: Website URL
            html_snippet: First portion of HTML content (will be truncated to minimize tokens)
            title: Page title
            weakness_score: Pre-calculated weakness score
            weakness_reasons: Pre-detected weakness reasons

        Returns:
            Tuple of (is_weak: bool, reason: str, confidence: int 1-10)
        """
        if not html_snippet:
            return False, "コンテンツ取得失敗", 0

        # Truncate HTML to minimize tokens (first 1000 chars)
        truncated_html = html_snippet[:1000] if len(html_snippet) > 1000 else html_snippet

        # Format existing weakness info if available
        existing_info = ""
        if weakness_score > 0 or weakness_reasons:
            existing_info = f"""
既存の分析結果:
- 弱みスコア: {weakness_score}/100
- 検出された弱点: {', '.join(weakness_reasons or [])}
"""

        prompt = f"""あなたはWebサイト評価の専門家です。以下のサイトが「営業しやすい弱いサイト」かどうか判定してください。

URL: {url}
タイトル: {title}
{existing_info}
サイトの一部:
{truncated_html}

判定基準（弱いサイト = YES）:
- デザインが古い・安っぽい
- コンテンツが薄い（文字数少ない）
- 画像が少ない・質が低い
- 予約システムがない
- 料金が不明確
- プロフィールが薄い
- 連絡先が不明確
- 更新されていない

判定基準（強いサイト = NO）:
- デザインがプロ仕様
- コンテンツが充実
- 予約システム完備
- 料金明確
- しっかりしたプロフィール

以下の形式で回答してください:
判定: YES or NO
理由: (1行で簡潔に)
確信度: 1-10"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "あなたはWebサイト評価の専門家です。簡潔に回答してください。"
                    },
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.3
            )

            result = response.choices[0].message.content.strip()
            logger.debug(f"AI response for {url}: {result}")

            # Parse result
            lines = result.split('\n')

            # Extract is_weak (YES/NO)
            is_weak = False
            for line in lines:
                if '判定' in line:
                    is_weak = 'YES' in line.upper()
                    break

            # Extract reason
            reason = "理由不明"
            for line in lines:
                if '理由' in line and ':' in line:
                    reason = line.split(':', 1)[1].strip()
                    break

            # Extract confidence (1-10)
            confidence = 7  # Default
            for line in lines:
                if '確信度' in line:
                    try:
                        # Extract digits from line
                        digits = ''.join(filter(str.isdigit, line))
                        if digits:
                            confidence = min(10, max(1, int(digits[:2])))  # Clamp 1-10
                    except (ValueError, IndexError):
                        pass
                    break

            logger.info(f"AI verified {url}: weak={is_weak}, confidence={confidence}")
            return is_weak, reason, confidence

        except openai.RateLimitError as e:
            logger.warning(f"OpenAI rate limit exceeded: {e}")
            return False, "API制限超過", 0
        except openai.APIError as e:
            logger.error(f"OpenAI API error: {e}")
            return False, f"APIエラー: {str(e)[:30]}", 0
        except Exception as e:
            logger.error(f"AI verification failed for {url}: {e}")
            return False, f"エラー: {str(e)[:30]}", 0

    def batch_verify(
        self,
        leads: List[Dict],
        top_n: int = 30,
        min_confidence: int = 6
    ) -> Tuple[List[Dict], Dict]:
        """
        Verify top N leads sorted by weakness_score, return ALL leads with AI fields.

        Args:
            leads: List of lead dictionaries
            top_n: Number of top leads to verify (default 30)
            min_confidence: Minimum confidence to include in verified results (default 6)

        Returns:
            Tuple of (all_leads_with_ai_fields, stats_dict)
            - All leads are returned, with AI verification fields added to checked ones
            - Leads are sorted: AI-confirmed weak first, then unchecked, then AI-confirmed strong
        """
        if not leads:
            return [], {'total': 0, 'verified': 0, 'confirmed_weak': 0}

        # Sort by weakness_score descending
        sorted_leads = sorted(
            leads,
            key=lambda x: int(x.get('weakness_score', 0)),
            reverse=True
        )

        # Take top N for AI verification
        top_leads = sorted_leads[:top_n]
        remaining_leads = sorted_leads[top_n:]

        stats = {
            'total': len(leads),
            'checked': 0,
            'confirmed_weak': 0,
            'confirmed_strong': 0,
            'errors': 0,
            'avg_confidence': 0.0
        }

        ai_weak_leads = []      # AI confirmed weak with high confidence
        ai_strong_leads = []    # AI confirmed strong
        ai_error_leads = []     # AI verification failed
        total_confidence = 0

        for lead in top_leads:
            url = lead.get('url', '')
            html = lead.get('html', '') or lead.get('visible_text', '')
            title = lead.get('title', '') or lead.get('shop_name', '')
            weakness_score = int(lead.get('weakness_score', 0))
            weakness_reasons = lead.get('weakness_reasons', [])
            if isinstance(weakness_reasons, str):
                weakness_reasons = [r.strip() for r in weakness_reasons.split(';') if r.strip()]

            is_weak, reason, confidence = self.verify_weak_site(
                url=url,
                html_snippet=html,
                title=title,
                weakness_score=weakness_score,
                weakness_reasons=weakness_reasons
            )

            stats['checked'] += 1

            # Add AI verification results to lead
            lead['ai_verified'] = is_weak
            lead['ai_reason'] = reason
            lead['ai_confidence'] = confidence

            if confidence > 0:
                total_confidence += confidence

                if is_weak:
                    stats['confirmed_weak'] += 1
                    ai_weak_leads.append(lead)
                else:
                    stats['confirmed_strong'] += 1
                    ai_strong_leads.append(lead)
            else:
                stats['errors'] += 1
                ai_error_leads.append(lead)

        # Mark remaining leads as unchecked
        for lead in remaining_leads:
            lead['ai_verified'] = None  # Not checked
            lead['ai_reason'] = '未検証'
            lead['ai_confidence'] = 0

        # Calculate average confidence
        if stats['checked'] - stats['errors'] > 0:
            stats['avg_confidence'] = round(
                total_confidence / (stats['checked'] - stats['errors']),
                1
            )

        # Sort AI-confirmed weak leads by confidence (high first)
        ai_weak_leads.sort(key=lambda x: x.get('ai_confidence', 0), reverse=True)

        # Combine all leads in priority order:
        # 1. AI-confirmed weak (sorted by confidence)
        # 2. AI errors (verification failed, keep original score order)
        # 3. Unchecked leads (original score order)
        # 4. AI-confirmed strong (still potentially useful)
        all_leads = ai_weak_leads + ai_error_leads + remaining_leads + ai_strong_leads

        logger.info(f"AI batch verification complete: {stats}")

        return all_leads, stats


    def verify_relevance(
        self,
        url: str,
        title: str,
        snippet: str,
        target_btypes: List[str],
        target_location: str,
    ) -> Dict:
        """
        Verify if a URL is relevant to the target business types and location.

        Args:
            url: Website URL
            title: Page title or snippet title
            snippet: Text snippet (max 600 chars)
            target_btypes: List of target business type strings
            target_location: Target prefecture/city string

        Returns:
            Dict with keys: keep, reason, category, confidence
        """
        default_keep = {"keep": True, "reason": "判定不能のため保持", "category": "other", "confidence": 1}

        if not url:
            return default_keep

        rule_result = _rule_based_relevance(url, title, snippet, target_btypes, target_location)
        if rule_result is not None:
            return rule_result

        truncated_snippet = snippet[:800] if snippet else ""
        btypes_str = "、".join(target_btypes[:10])

        user_prompt = f"""以下のURLが日本のローカルサービス事業者のサイトかどうか判定してください。

URL: {url}
タイトル: {title}
テキスト抜粋: {truncated_snippet}
ターゲット業種: {btypes_str}
ターゲット地域: {target_location}

判定ルール:
- 拒否: 明らかに海外/外国語のみのサイト
- 拒否: ポータル/ディレクトリ/一覧サイト（ホットペッパー、楽天、エキテン等）
- 拒否: SNSプロフィール/リンクまとめ（Instagram、Twitter、lit.link等）
- 拒否: ECサイトのみ（予約・サービス提供なし）
- 拒否: 無関係の業種（ソフトウェア、製造、学校、病院等。ただしターゲット業種に含まれる場合は許可）
- 拒否: 求人サイト
- 許可: ターゲット業種に合致する日本のローカルサービス事業者

以下のJSON形式のみで回答してください:
{{"keep": true/false, "reason": "短い理由", "category": "ok|foreign|portal|sns|unrelated|ecommerce|job|other", "confidence": 1-10}}"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a strict lead-qualification classifier for Japanese local service businesses. Output JSON only."
                    },
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=120,
                temperature=0.1,
            )

            result_text = response.choices[0].message.content.strip()
            logger.debug(f"AI relevance response for {url}: {result_text}")

            parsed = _extract_json_object(result_text)
            if not parsed:
                logger.warning(f"AI relevance parse error for {url}: no JSON object")
                return default_keep

            # Validate required fields
            raw_cat = str(parsed.get("category", "other")).lower()
            cat_map = {
                "ok": "ok",
                "foreign": "foreign",
                "overseas": "foreign",
                "portal": "portal",
                "sns": "sns",
                "directory": "directory",
                "job": "job",
                "job_listing": "job",
                "ecommerce": "ecommerce",
                "unrelated": "unrelated",
                "other": "other",
            }
            norm_cat = cat_map.get(raw_cat, "other")
            result = {
                "keep": bool(parsed.get("keep", True)),
                "reason": str(parsed.get("reason", ""))[:100],
                "category": norm_cat,
                "confidence": max(1, min(10, int(parsed.get("confidence", 5)))),
            }
            # Safety: if model says DROP but no actionable category, keep.
            if not result["keep"] and result["category"] == "other":
                result["keep"] = True
                result["reason"] = "low_specificity_keep"
                result["confidence"] = min(result["confidence"], 5)
            logger.info(f"AI relevance {url}: keep={result['keep']}, cat={result['category']}, conf={result['confidence']}")
            return result

        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"AI relevance parse error for {url}: {e}")
            return default_keep
        except Exception as e:
            logger.error(f"AI relevance check failed for {url}: {e}")
            return default_keep

    def batch_verify_relevance(
        self,
        urls_with_meta: List[Dict],
        target_btypes: List[str],
        target_location: str,
        min_confidence: int = 6,
    ) -> Tuple[List[Dict], Dict]:
        """
        Verify relevance of multiple URLs. Returns kept URLs and stats.

        Args:
            urls_with_meta: List of dicts with keys: url, title, snippet
            target_btypes: Target business type strings
            target_location: Target prefecture/city
            min_confidence: Minimum confidence to act on (below this, keep URL)

        Returns:
            Tuple of (kept_url_dicts, stats_dict)
        """
        if not urls_with_meta:
            return [], {"total": 0, "kept": 0, "dropped": 0, "by_category": {}}

        stats = {
            "total": len(urls_with_meta),
            "kept": 0,
            "dropped": 0,
            "by_category": {},
        }

        kept = []
        for item in urls_with_meta:
            url = item.get("url", "")
            title = item.get("title", "")
            snippet = item.get("snippet", "")

            result = self.verify_relevance(
                url=url,
                title=title,
                snippet=snippet,
                target_btypes=target_btypes,
                target_location=target_location,
            )

            cat = result.get("category", "other")
            confidence = result.get("confidence", 1)

            # Safety: if confidence is below threshold, do NOT drop
            if not result.get("keep", True) and confidence >= min_confidence:
                stats["dropped"] += 1
                stats["by_category"][cat] = stats["by_category"].get(cat, 0) + 1
                logger.debug(f"AI relevance DROP: {url} -> {cat} ({result.get('reason', '')})")
            else:
                stats["kept"] += 1
                kept.append(item)

        logger.info(
            f"AI relevance batch: {stats['total']} checked, "
            f"{stats['kept']} kept, {stats['dropped']} dropped. "
            f"By category: {stats['by_category']}"
        )
        return kept, stats


    # Valid AI filter flags
    AI_FILTER_FLAGS = [
        "OVERSEAS", "NON_JP_LANG", "PORTAL", "SNS", "DIRECTORY",
        "JOB_LISTING", "PDF_OR_FILE", "IRRELEVANT_INDUSTRY", "NO_LOCAL_SIGNAL",
    ]

    def filter_relevance(
        self,
        lead: Dict,
        target_btypes: List[str],
        target_location: str,
    ) -> Dict:
        """
        Post-crawl relevance filter for a single lead.

        Analyzes a lead's actual content (title, visible_text, URL) to determine
        whether it should be kept or dropped.

        Args:
            lead: Lead dict with url, title/shop_name, visible_text/html
            target_btypes: Target business types
            target_location: Target prefecture/city

        Returns:
            Dict with keys: ai_action (KEEP/DROP), ai_flags (list of flag strings),
            ai_filter_reason (str), ai_filter_confidence (int 1-10)
        """
        default_keep = {
            "ai_action": "KEEP",
            "ai_flags": [],
            "ai_filter_reason": "判定不能のため保持",
            "ai_filter_confidence": 1,
        }

        url = lead.get("url", "")
        title = lead.get("title", "") or lead.get("shop_name", "") or lead.get("store_name", "")
        text = lead.get("visible_text", "") or lead.get("html", "")

        if not url:
            return default_keep

        # Rule-based guardrail first (fast and reliable).
        rule_result = _rule_based_relevance(url, title, text[:600] if text else "", target_btypes, target_location)
        if rule_result is not None and not rule_result.get("keep", True):
            cat = str(rule_result.get("category", "other")).lower()
            cat_to_flag = {
                "foreign": "OVERSEAS",
                "portal": "PORTAL",
                "sns": "SNS",
                "directory": "DIRECTORY",
                "job": "JOB_LISTING",
            }
            mapped_flag = cat_to_flag.get(cat)
            if mapped_flag:
                return {
                    "ai_action": "DROP",
                    "ai_flags": [mapped_flag],
                    "ai_filter_reason": str(rule_result.get("reason", "rule_drop"))[:100],
                    "ai_filter_confidence": max(1, min(10, int(rule_result.get("confidence", 8)))),
                }

        # Truncate text to save tokens
        truncated_text = text[:800] if text else ""
        btypes_str = "、".join(target_btypes[:10])
        flags_str = ", ".join(self.AI_FILTER_FLAGS)

        user_prompt = f"""以下のリード（サイト）がターゲット業種のローカルサービス事業者かどうか判定してください。

URL: {url}
タイトル: {title}
テキスト抜粋: {truncated_text}
ターゲット業種: {btypes_str}
ターゲット地域: {target_location}

DROP判定フラグ（該当するものをすべて返してください）:
- OVERSEAS: 海外サイト、外国のサービス
- NON_JP_LANG: 日本語がほぼ含まれない
- PORTAL: ポータル/ディレクトリ/一覧サイト（ホットペッパー、楽天、エキテン等）
- SNS: SNSプロフィール/リンクまとめ（Instagram、Twitter、lit.link等）
- DIRECTORY: 口コミサイト、ランキングサイト、まとめサイト
- JOB_LISTING: 求人サイト
- PDF_OR_FILE: PDF/ファイルダウンロード（非Webページ）
- IRRELEVANT_INDUSTRY: ターゲット業種と無関係（製造、学校、病院等）
- NO_LOCAL_SIGNAL: 地域密着型サービスのシグナルなし

判定ルール:
- フラグが1つでも該当すればDROP
- フラグなしならKEEP
- 確信度が低い場合はKEEPを優先

以下のJSON形式のみで回答:
{{"action": "KEEP" or "DROP", "flags": [{flags_str}から該当するもの], "reason": "短い理由", "confidence": 1-10}}"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a strict lead-qualification classifier for Japanese local service businesses. Output JSON only.",
                    },
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=150,
                temperature=0.1,
            )

            result_text = response.choices[0].message.content.strip()
            logger.debug(f"AI filter response for {url}: {result_text}")

            parsed = _extract_json_object(result_text)
            if not parsed:
                logger.warning(f"AI filter parse error for {url}: no JSON object")
                return default_keep

            action = "DROP" if str(parsed.get("action", "KEEP")).upper() == "DROP" else "KEEP"
            raw_flags = parsed.get("flags", [])
            # Validate flags
            flags = [f for f in raw_flags if f in self.AI_FILTER_FLAGS]
            reason = str(parsed.get("reason", ""))[:100]
            confidence = max(1, min(10, int(parsed.get("confidence", 5))))

            # Safety: avoid dropping relevant local salons on weak reasoning.
            hard_drop_flags = {"OVERSEAS", "NON_JP_LANG", "PORTAL", "SNS", "DIRECTORY", "JOB_LISTING", "PDF_OR_FILE", "IRRELEVANT_INDUSTRY"}
            if action == "DROP":
                if not flags:
                    action = "KEEP"
                    reason = "flag無しのDROPは無効化"
                    confidence = min(confidence, 5)
                elif all(f == "NO_LOCAL_SIGNAL" for f in flags):
                    action = "KEEP"
                    reason = "NO_LOCAL_SIGNAL単独はKEEP"
                    confidence = min(confidence, 5)
                elif not any(f in hard_drop_flags for f in flags):
                    action = "KEEP"
                    reason = "hard_drop根拠不足のためKEEP"
                    confidence = min(confidence, 5)

            result = {
                "ai_action": action,
                "ai_flags": flags,
                "ai_filter_reason": reason,
                "ai_filter_confidence": confidence,
            }
            logger.info(f"AI filter {url}: action={action}, flags={flags}, conf={confidence}")
            return result

        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"AI filter parse error for {url}: {e}")
            return default_keep
        except Exception as e:
            logger.error(f"AI filter failed for {url}: {e}")
            return default_keep

    def batch_filter_relevance(
        self,
        leads: List[Dict],
        top_n: int = 30,
        target_btypes: List[str] = None,
        target_location: str = "",
        min_confidence: int = 6,
    ) -> Tuple[List[Dict], Dict]:
        """
        Post-crawl relevance filter on leads. Runs filter_relevance on top_n leads,
        attaches AI fields to every lead, and returns all leads with stats.

        Leads that are DROPped with sufficient confidence are moved to the end.
        Remaining leads keep their original order.

        Args:
            leads: List of lead dicts (post-crawl, with visible_text etc.)
            top_n: Number of leads to check (sorted by weakness_score desc)
            target_btypes: Target business types
            target_location: Target prefecture/city
            min_confidence: Minimum confidence to act on DROP decision

        Returns:
            Tuple of (all_leads_with_ai_fields, stats_dict)
        """
        if not leads:
            return [], {"total": 0, "checked": 0, "kept": 0, "dropped": 0, "by_flag": {}}

        target_btypes = target_btypes or []

        # Prioritize risky/noisy leads first for AI filtering.
        def _risk_rank(lead: Dict) -> Tuple[int, int]:
            url = str(lead.get("url", "")).lower()
            txt = str(lead.get("visible_text", "") or lead.get("title", "") or "").lower()
            risk = 0
            if any(d in url for d in PORTAL_SNS_DOMAIN_HINTS):
                risk += 3
            if any(url.endswith(tld) for tld in FOREIGN_TLD_HINTS):
                risk += 3
            if not _contains_japanese(txt):
                risk += 2
            if any(k in txt for k in ["ranking", "ランキング", "一覧", "review", "口コミ", "求人", "recruit"]):
                risk += 2
            return (risk, int(lead.get("weakness_score", 0)))

        sorted_leads = sorted(leads, key=_risk_rank, reverse=True)

        to_check = sorted_leads[:top_n]
        unchecked = sorted_leads[top_n:]

        stats = {
            "total": len(leads),
            "checked": 0,
            "kept": 0,
            "dropped": 0,
            "errors": 0,
            "by_flag": {},
        }

        kept_leads = []
        dropped_leads = []

        for lead in to_check:
            result = self.filter_relevance(
                lead=lead,
                target_btypes=target_btypes,
                target_location=target_location,
            )

            # Attach AI filter fields to lead
            lead["ai_action"] = result["ai_action"]
            lead["ai_flags"] = result["ai_flags"]
            lead["ai_filter_reason"] = result["ai_filter_reason"]
            lead["ai_filter_confidence"] = result["ai_filter_confidence"]

            stats["checked"] += 1
            confidence = result["ai_filter_confidence"]

            if result["ai_action"] == "DROP" and confidence >= min_confidence:
                stats["dropped"] += 1
                # Track flag counts
                for flag in result["ai_flags"]:
                    stats["by_flag"][flag] = stats["by_flag"].get(flag, 0) + 1
                dropped_leads.append(lead)
            else:
                stats["kept"] += 1
                kept_leads.append(lead)

            if confidence == 0:
                stats["errors"] += 1

        # Mark unchecked leads
        for lead in unchecked:
            lead["ai_action"] = "KEEP"
            lead["ai_flags"] = []
            lead["ai_filter_reason"] = "未検証"
            lead["ai_filter_confidence"] = 0

        # Order: kept first, then unchecked, then dropped
        all_leads = kept_leads + unchecked + dropped_leads

        logger.info(
            f"AI filter batch: {stats['total']} total, {stats['checked']} checked, "
            f"{stats['kept']} kept, {stats['dropped']} dropped. "
            f"By flag: {stats['by_flag']}"
        )

        return all_leads, stats


def verify_leads_with_ai(
    leads: List[Dict],
    api_key: str = None,
    top_n: int = 30,
    min_confidence: int = 6
) -> Tuple[List[Dict], Dict]:
    """
    Convenience function to verify leads with AI.

    Args:
        leads: List of lead dictionaries
        api_key: OpenAI API key (optional, uses env var if not provided)
        top_n: Number of top leads to verify
        min_confidence: Minimum confidence threshold

    Returns:
        Tuple of (verified_leads, stats_dict)
    """
    try:
        verifier = AIVerifier(api_key=api_key)
        return verifier.batch_verify(leads, top_n=top_n, min_confidence=min_confidence)
    except (ImportError, ValueError) as e:
        logger.warning(f"AI verification unavailable: {e}")
        return leads[:top_n], {'error': str(e)}
