"""
CSV output writer for leads data.

Supports two modes:
1. Legacy mode: Original column schema (for backward compatibility)
2. Normalized mode: New clean schema with dedup and sorting (default)
"""
import csv
import logging
import os
from typing import List, Dict, Optional

from src.normalize import (
    normalize_leads,
    get_schema_columns,
    get_header_labels,
    FINAL_SCHEMA,
    HEADER_LABELS as NORMALIZED_HEADERS,
)

logger = logging.getLogger(__name__)


class OutputWriter:
    """Writes leads to CSV file."""

    # Legacy columns (kept for backward compatibility)
    LEGACY_COLUMNS = [
        'shop_name', 'url', 'grade', 'score',
        'business_type', 'owner_name', 'phone', 'email',
        'address', 'city', 'business_hours', 'domain',
        'site_type', 'reasons',
    ]

    LEGACY_HEADER_LABELS = {
        'shop_name': '店舗名',
        'url': '元のURL',
        'grade': '判定',
        'score': '点数',
        'business_type': '業種',
        'owner_name': 'オーナー名',
        'phone': '電話番号',
        'email': 'メール',
        'address': '住所',
        'city': '市区町村',
        'business_hours': '営業時間',
        'domain': 'ドメイン',
        'site_type': 'サイト種別',
        'reasons': '判定理由',
    }

    @staticmethod
    def write_csv(
        leads: List[Dict],
        output_path: str,
        encoding: str = 'utf-8-sig',
        normalize: bool = True,
        source_query: str = '',
        region: str = ''
    ):
        """
        Write leads to CSV file.

        Args:
            leads: List of lead dictionaries
            output_path: Path to output CSV file
            encoding: File encoding (default utf-8-sig for BOM)
            normalize: If True, apply normalization pipeline (default True)
            source_query: Search query for normalized output
            region: Region name for normalized output
        """
        # Create output directory if needed
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        try:
            if normalize:
                # Use new normalized schema
                OutputWriter._write_normalized_csv(
                    leads, output_path, encoding, source_query, region
                )
            else:
                # Use legacy schema
                OutputWriter._write_legacy_csv(leads, output_path, encoding)

        except Exception as e:
            logger.error(f"Failed to write CSV to {output_path}: {e}")
            raise

    @staticmethod
    def _write_normalized_csv(
        leads: List[Dict],
        output_path: str,
        encoding: str,
        source_query: str,
        region: str
    ):
        """Write CSV using normalized schema with dedup and sorting."""
        # Apply normalization pipeline
        normalized_leads = normalize_leads(leads, source_query=source_query, region=region)

        columns = get_schema_columns()
        headers = get_header_labels()

        with open(output_path, 'w', encoding=encoding, newline='') as f:
            # Write Japanese header row
            header_row = [headers.get(col, col) for col in columns]
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(header_row)

            # Write data rows
            for lead in normalized_leads:
                row = [lead.get(col, '') for col in columns]
                writer.writerow(row)

        logger.info(f"Saved {len(normalized_leads)} normalized leads to {output_path}")

    @staticmethod
    def _write_legacy_csv(leads: List[Dict], output_path: str, encoding: str):
        """Write CSV using legacy schema (backward compatibility)."""
        with open(output_path, 'w', encoding=encoding, newline='') as f:
            # Write Japanese header row
            header = [
                OutputWriter.LEGACY_HEADER_LABELS.get(col, col)
                for col in OutputWriter.LEGACY_COLUMNS
            ]
            writer_simple = csv.writer(f)
            writer_simple.writerow(header)

            # Write rows
            writer = csv.DictWriter(f, fieldnames=OutputWriter.LEGACY_COLUMNS)
            for lead in leads:
                row = {
                    col: (lead.get(col, '') if lead.get(col) is not None else '')
                    for col in OutputWriter.LEGACY_COLUMNS
                }
                writer.writerow(row)

        logger.info(f"Saved {len(leads)} leads (legacy format) to {output_path}")

    @staticmethod
    def write_failed_urls(failed_urls: List[str], output_path: str):
        """
        Write failed URLs to text file for retry.

        Args:
            failed_urls: List of failed URLs
            output_path: Path to output file
        """
        if not failed_urls:
            return

        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            with open(output_path, 'w', encoding='utf-8') as f:
                for url in failed_urls:
                    f.write(f"{url}\n")

            logger.info(f"Saved {len(failed_urls)} failed URLs to {output_path}")

        except Exception as e:
            logger.error(f"Failed to write failed URLs to {output_path}: {e}")
