"""
Lead processor that coordinates crawling, scoring, and deduplication.
"""
import logging
from typing import List, Dict, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from src.crawler import WebCrawler
from src.scorer import WebsiteScorer
from src.content_analyzer import ContentAnalyzer
from src.utils.url_filter import normalize_url, extract_domain
from src.filters import filter_leads, is_relevant_lead, get_filter_reason
from src.scoring_rules import apply_scoring_boost, compute_weakness, apply_solo_boost_to_leads
from src.solo_classifier import SoloClassifier
from src.normalize import normalize_leads, deduplicate_leads, map_to_final_schema

logger = logging.getLogger(__name__)

# Workaround: Some environments (Flask background threads, redirected stderr)
# can make `tqdm` call into `sys.stderr.flush()` which raises OSError
# (Errno 22) on Windows. Monkeypatch `tqdm.std.status_printer` to a
# no-op safe printer when available to avoid crashing the background
# search thread during URL processing.
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
    # If tqdm isn't installed or monkeypatching fails, continue silently.
    pass

# Additionally, replace tqdm.tqdm with a no-op if available so any
# accidental creation of progress bars in worker threads is harmless.
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
# Lazy import for AI verifier (optional dependency)
_ai_verifier_class = None
_ai_verifier_checked = False


def _get_ai_verifier_class():
    """Lazy load AI verifier class to avoid import errors if openai not installed."""
    global _ai_verifier_class, _ai_verifier_checked
    if not _ai_verifier_checked:
        _ai_verifier_checked = True
        try:
            from src.ai_verifier import AIVerifier
            _ai_verifier_class = AIVerifier
        except (ImportError, ValueError) as e:
            logger.warning(f"AI verifier not available: {e}")
            _ai_verifier_class = None
    return _ai_verifier_class


class LeadProcessor:
    """Processes URLs into scored leads."""

    def __init__(self, parallel_workers: int = 10, disable_progress: bool = False):
        self.crawler = WebCrawler()
        self.scorer = WebsiteScorer()
        self.analyzer = ContentAnalyzer()
        self.solo_classifier = SoloClassifier(self.crawler.session)
        self.parallel_workers = parallel_workers
        self.disable_progress = disable_progress

    def process_url(self, url: str) -> Dict:
        """
        Process a single URL: crawl, extract, analyze, and score.

        Args:
            url: URL to process

        Returns:
            Dictionary with lead data, or None if failed/aggregator
        """
        logger.info(f"Processing: {url}")

        # Validate and fetch home page
        inspection = self.solo_classifier.inspect_home(url)
        if not inspection.ok:
            normalized = inspection.normalized_url or url
            return {
                'shop_name': '',
                'business_type': '',
                'owner_name': '',
                'phone': '',
                'address': '',
                'city': '',
                'business_hours': '',
                'email': '',
                'url': normalized,
                'domain': extract_domain(normalized),
                'site_type': '',
                'score': None,
                'grade': 'C',
                'reasons': '',
                'title': '',
                'visible_text': '',
                'html': '',
                'solo_score': None,
                'solo_classification': inspection.url_status.lower(),
                'solo_reasons': [],
                'solo_evidence_snippets': [],
                'solo_detected_corp_terms': [],
                'url_status': inspection.url_status,
                'error_code': inspection.error_code,
            }

        # Use fetched HTML for analysis and scoring
        html = inspection.html
        extracted = self.crawler.extract_data(url, html)
        if not extracted:
            logger.warning(f"Failed to extract data: {url}")
            return None

        # Parse HTML for analysis
        soup = BeautifulSoup(html, 'html.parser')

        # Analyze content to detect aggregators and extract business info
        analysis = self.analyzer.analyze(url, html, soup, extracted)

        # FILTER OUT AGGREGATORS
        if analysis['is_aggregator']:
            logger.info(f"  Skipping aggregator site: {url}")
            return None

        # Score
        score_data = self.scorer.score(url, html, extracted)

        # Get domain
        domain = extract_domain(url)

        # Solo proprietor likelihood scoring (lightweight crawl)
        solo_result = self.solo_classifier.classify(inspection.normalized_url or url, html)

        # Combine data with improved shop name and additional fields
        lead = {
            'shop_name': analysis['shop_name'],  # Improved name extraction
            'business_type': analysis['business_type'],
            'owner_name': analysis['owner_name'],
            'phone': analysis['phone'],
            'address': analysis['address'],
            'city': extracted.get('city_guess', ''),
            'business_hours': analysis['business_hours'],
            'email': extracted.get('contact_email', ''),
            'url': url,
            'domain': domain,
            'site_type': score_data['site_type'],
            'score': score_data['score'],
            'grade': score_data['grade'],
            'reasons': score_data['reasons'],
            'title': extracted.get('title', ''),
            'visible_text': extracted.get('visible_text', ''),
            'html': html,
            'solo_score': solo_result.get('solo_score'),
            'solo_classification': solo_result.get('classification', 'unknown'),
            'solo_reasons': solo_result.get('reasons', []),
            'solo_evidence_snippets': solo_result.get('evidence_snippets', []),
            'solo_detected_corp_terms': solo_result.get('detected_corp_terms', []),
            'url_status': inspection.url_status,
            'error_code': inspection.error_code,
        }

        logger.info(f"  OK: {analysis['shop_name']} - Score: {lead['score']} ({lead['grade']})")

        return lead

    def process_urls(self, urls: List[str]) -> Tuple[List[Dict], List[str]]:
        """
        Process multiple URLs in parallel.

        Args:
            urls: List of URLs to process

        Returns:
            Tuple of (leads list, failed_urls list)
        """
        leads = []
        failed_urls = []
        skipped_aggregators = 0

        logger.info(f"Processing {len(urls)} URLs with {self.parallel_workers} workers")

        try:
            with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                # Submit all tasks
                future_to_url = {
                    executor.submit(self.process_url, url): url
                    for url in urls
                }

                # Process results (skip tqdm entirely in web context to avoid stderr issues)
                processed_count = 0
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        lead = future.result()
                        if lead:
                            leads.append(lead)
                        else:
                            # Could be failed or aggregator
                            failed_urls.append(url)
                    except OSError as e:
                        # OSError [Errno 22] can happen with tqdm/stderr on Windows
                        logger.warning(f"OSError processing {url}: {e}")
                        failed_urls.append(url)
                    except Exception as e:
                        logger.error(f"Error processing {url}: {e}")
                        failed_urls.append(url)
                    finally:
                        processed_count += 1
                        if not self.disable_progress and processed_count % 10 == 0:
                            logger.info(f"Progress: {processed_count}/{len(urls)} URLs processed")
        except OSError as e:
            # Catch any OSError from ThreadPoolExecutor itself (e.g., tqdm stderr issues)
            logger.warning(f"OSError in ThreadPoolExecutor (continuing with partial results): {e}")

        logger.info(f"Successfully processed {len(leads)} individual business leads")
        logger.info(f"Filtered out aggregator/portal sites automatically")

        return leads, failed_urls

    def deduplicate_leads(self, leads: List[Dict]) -> List[Dict]:
        """
        Deduplicate leads by normalized URL and domain.
        Keep highest-scored lead for each domain.

        Args:
            leads: List of lead dictionaries

        Returns:
            Deduplicated list of leads
        """
        logger.info(f"Deduplicating {len(leads)} leads")

        # Group by domain
        domain_to_leads = {}
        for lead in leads:
            domain = lead['domain']
            if domain not in domain_to_leads:
                domain_to_leads[domain] = []
            domain_to_leads[domain].append(lead)

        # Keep highest scored lead for each domain
        unique_leads = []
        for domain, domain_leads in domain_to_leads.items():
            # Sort by score descending (None treated as 0)
            domain_leads.sort(key=lambda x: int(x.get('score') or 0), reverse=True)
            unique_leads.append(domain_leads[0])

        logger.info(f"After deduplication: {len(unique_leads)} unique leads")

        return unique_leads

    def filter_and_boost(self, leads: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Apply hard filters and scoring boost to leads.

        Pipeline order (CRITICAL: solo boost BEFORE filtering):
        1. Exclude invalid/blocked leads
        2. Apply SOLO score boost FIRST (prevents filtering out solo practitioners)
        3. Apply hard exclusion filters
        4. Apply general scoring boost for good signals
        5. Re-calculate grades based on new scores
        6. Compute weakness scoring
        7. Sort by weakness_score (desc), then score (desc)

        Args:
            leads: List of lead dictionaries

        Returns:
            Tuple of (kept_leads, filtered_leads)
        """
        logger.info(f"Applying filters and scoring boost to {len(leads)} leads")

        # Step 1: Exclude invalid/blocked leads from scoring pipeline
        invalid_status = {'invalid', 'blocked'}
        invalid_leads = []
        valid_leads = []
        for lead in leads:
            classification = str(lead.get('solo_classification', '')).lower()
            if classification in invalid_status:
                lead['filter_reason'] = lead.get('error_code', 'invalid_url')
                invalid_leads.append(lead)
            else:
                valid_leads.append(lead)

        # Step 2: Apply SOLO score boost FIRST (before hard filters)
        # This ensures solo/small practitioners get boosted scores before filtering
        valid_leads = apply_solo_boost_to_leads(valid_leads)
        solo_boosted = sum(1 for l in valid_leads if l.get('solo_boost', 0) > 0)
        logger.info(f"Solo boost applied to {solo_boosted} leads")

        # Step 3: Apply hard filters
        kept_leads, filtered_leads = filter_leads(valid_leads)
        filtered_leads.extend(invalid_leads)
        logger.info(f"After hard filter: {len(kept_leads)} kept, {len(filtered_leads)} filtered")

        # Step 4: Apply general scoring boost
        kept_leads = apply_scoring_boost(kept_leads)

        # Step 5: Re-calculate grades based on new scores
        for lead in kept_leads:
            score = int(lead.get('score', 0))
            if score >= 60:
                lead['grade'] = 'A'
            elif score >= 40:
                lead['grade'] = 'B'
            else:
                lead['grade'] = 'C'

        # Step 6: Compute weakness scoring (improvement opportunity)
        kept_leads = compute_weakness(kept_leads)

        # Step 7: Sort primarily by weakness_score (desc), then by fit score (desc)
        kept_leads.sort(key=lambda x: (int(x.get('weakness_score', 0)), int(x.get('score', 0))), reverse=True)

        logger.info(f"After scoring boost: top score={kept_leads[0]['score'] if kept_leads else 0}")

        return kept_leads, filtered_leads

    def apply_ai_verification(
        self,
        leads: List[Dict],
        top_n: int = 30,
        min_confidence: int = 6,
        api_key: str = None
    ) -> Tuple[List[Dict], Dict]:
        """
        Apply AI verification to top-scoring leads using GPT-4o-mini.

        Args:
            leads: List of lead dictionaries
            top_n: Number of top leads to verify (default 30)
            min_confidence: Minimum AI confidence to include (default 6)
            api_key: OpenAI API key (optional, uses env var if not provided)

        Returns:
            Tuple of (verified_leads, stats_dict)
        """
        AIVerifierClass = _get_ai_verifier_class()
        if not AIVerifierClass:
            logger.warning("AI verification skipped: openai package not available")
            return leads, {'error': 'AI verifier not available'}

        try:
            verifier = AIVerifierClass(api_key=api_key)
            logger.info(f"Starting AI verification for top {top_n} leads...")

            verified_leads, stats = verifier.batch_verify(
                leads,
                top_n=top_n,
                min_confidence=min_confidence
            )

            logger.info(f"AI verification complete: {stats}")
            return verified_leads, stats

        except Exception as e:
            logger.error(f"AI verification failed: {e}")
            return leads, {'error': str(e)}

    def process_pipeline(
        self,
        urls: List[str],
        use_ai_verify: bool = False,
        ai_top_n: int = 30,
        ai_api_key: str = None
    ) -> Tuple[List[Dict], List[str], List[Dict]]:
        """
        Full processing pipeline with improved filtering.

        Pipeline order:
        1. Process URLs (crawl, extract, basic score)
        2. Deduplicate by domain
        3. Apply hard filters
        4. Apply scoring boost
        5. Sort by score
        6. (Optional) AI verification of top leads

        Args:
            urls: List of URLs to process
            use_ai_verify: If True, apply AI verification to top leads
            ai_top_n: Number of top leads to verify with AI
            ai_api_key: OpenAI API key for verification

        Returns:
            Tuple of (final_leads, failed_urls, filtered_leads)
        """
        # Step 1: Process URLs (crawl, extract, basic score)
        raw_leads, failed_urls = self.process_urls(urls)

        # Step 2: Normalize raw leads to final schema and deduplicate by normalized URL
        # map raw leads to normalized schema (but keep original metadata in mapping)
        normalized = [map_to_final_schema(r) for r in raw_leads]

        # Step 3: Deduplicate by normalized URL (primary) and domain (secondary)
        deduped = deduplicate_leads(normalized)

        # Step 4: Apply hard filters and scoring boost
        final_leads, filtered_leads = self.filter_and_boost(deduped)

        # Step 5: (Optional) AI Verification of top leads
        ai_stats = None
        if use_ai_verify and final_leads:
            logger.info(f"Applying AI verification to top {ai_top_n} leads...")
            verified_leads, ai_stats = self.apply_ai_verification(
                final_leads,
                top_n=ai_top_n,
                api_key=ai_api_key
            )
            if 'error' not in ai_stats:
                # Replace with leads that have AI verification fields added
                # All leads are returned, sorted: AI-confirmed weak first, then others
                final_leads = verified_leads
                logger.info(f"AI verification: {ai_stats.get('confirmed_weak', 0)} confirmed weak, {ai_stats.get('confirmed_strong', 0)} confirmed strong")

        logger.info(f"Pipeline complete: {len(final_leads)} final leads")

        return final_leads, failed_urls, filtered_leads
