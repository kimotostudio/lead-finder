#!/usr/bin/env python3
"""
Fixed benchmark definitions for ops_cycle bulk verification.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class BenchSpec:
    name: str
    input_csv_path: str
    slice: int = 200
    mode: str = "B"
    loop: int = 1
    notes: str = ""


# NOTE:
# - Update tokyo/osaka paths to your latest stable benchmark CSVs as needed.
# - Missing files are reported as FAIL(input_missing) by tools.bench_run.
BENCHES: list[BenchSpec] = [
    BenchSpec(
        name="fukuoka_chuo",
        input_csv_path=str(ROOT / "web_app" / "output" / "leads_Fukuoka_福岡市中央区_20260214_201611.csv"),
        slice=200,
        mode="B",
        loop=1,
        notes="Primary benchmark used for mode B tuning.",
    ),
    BenchSpec(
        name="tokyo_xxx",
        input_csv_path=str(ROOT / "web_app" / "web_app" / "output" / "leads_東京_20260118_122237.csv"),
        slice=200,
        mode="B",
        loop=1,
        notes="Tokyo benchmark sample.",
    ),
    BenchSpec(
        name="osaka_xxx",
        input_csv_path=str(ROOT / "web_app" / "output" / "leads_Osaka_大阪市北区_20260214_000000.csv"),
        slice=200,
        mode="B",
        loop=1,
        notes="Placeholder path. Replace with an Osaka benchmark CSV.",
    ),
]


# Non-gating warning thresholds for potential overfitting.
UNKNOWN_DROP_WARN = 0.12
CORPORATE_INCREASE_WARN = 0.12
SOLO_INCREASE_WARN = 0.12

