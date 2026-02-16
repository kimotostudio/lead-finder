#!/usr/bin/env python3
"""
Minimal theta-space configuration for OPS cycle reproducibility.
"""

from __future__ import annotations

from typing import Any


NOISE_DOMAIN_SUFFIXES = (".go.jp", ".lg.jp")

GLOBAL_MEDIA_NOISE_DOMAINS = (
    "forbes.com",
    "cnn.com",
    "bbc.com",
    "reuters.com",
    "apnews.com",
)

OPS_AUTO_NOISE_DOMAINS: tuple[str, ...] = ()

NOISE_KEYWORDS = (
    "ランキング",
    "口コミ",
    "おすすめ",
    "評判",
    "人気",
    "一覧",
    "比較",
)

GATE_POLICY = {
    "require_bad_domain_mix_non_increasing": True,
    "require_city_missing_rate_non_worsening": True,
    "require_solo_rate_non_worsening": True,
    "allow_top50_good_drop_if_explained_by_noise_removed": True,
}


def normalize_domain(domain: str) -> str:
    value = (domain or "").strip().lower()
    if value.startswith("www."):
        return value[4:]
    return value


def get_theta_snapshot() -> dict[str, Any]:
    return {
        "noise_domain_suffixes": list(NOISE_DOMAIN_SUFFIXES),
        "global_media_noise_domains": list(GLOBAL_MEDIA_NOISE_DOMAINS),
        "ops_auto_noise_domains": list(OPS_AUTO_NOISE_DOMAINS),
        "noise_keywords": list(NOISE_KEYWORDS),
        "gate_policy": dict(GATE_POLICY),
    }
