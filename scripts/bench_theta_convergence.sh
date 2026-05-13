#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

OUTPUT_DIR="ops_runs/_reports"
mkdir -p "$OUTPUT_DIR"

MAX_FILES="${MAX_FILES:-12}"
LOOP_N="${LOOP_N:-10}"
CANDIDATES="${CANDIDATES:-3}"
STABILITY_SLICE="${STABILITY_SLICE:-200}"
NO_PROGRESS_K="${NO_PROGRESS_K:-2}"

TS="$(date +%Y%m%d_%H%M%S)"
SELECTED_LIST="$OUTPUT_DIR/theta_convergence_inputs_${TS}.txt"
RUNS_TSV="$OUTPUT_DIR/theta_convergence_runs_${TS}.tsv"

mapfile -t ALL_CSVS < <(find "web_app/output" -maxdepth 1 -type f -name 'leads_*.csv' | LC_ALL=C sort)
if [[ "${#ALL_CSVS[@]}" -eq 0 ]]; then
  echo "ERROR: no leads_*.csv found under web_app/output" >&2
  exit 1
fi

python3 - "$MAX_FILES" "${ALL_CSVS[@]}" > "$SELECTED_LIST" <<'PY'
import pathlib
import re
import sys

max_files = int(sys.argv[1])
paths = [pathlib.Path(p) for p in sys.argv[2:]]

def region_key(path: pathlib.Path) -> str:
    stem = path.stem
    parts = stem.split("_")
    # leads_<pref>_<city/ward>_<date>_<time>
    core = [p for p in parts[1:] if p]
    filtered = [p for p in core if not re.fullmatch(r"\d{6,}", p)]
    if len(filtered) >= 2:
        return f"{filtered[0]}::{filtered[1]}"
    if filtered:
        return filtered[0]
    return stem

selected = []
seen = set()
for p in paths:
    key = region_key(p)
    if key in seen:
        continue
    seen.add(key)
    selected.append(p)

if len(selected) < max_files:
    used = {str(p) for p in selected}
    for p in paths:
        if str(p) in used:
            continue
        selected.append(p)
        used.add(str(p))
        if len(selected) >= max_files:
            break

for p in selected[:max_files]:
    print(str(p))
PY

mapfile -t SELECTED_CSVS < "$SELECTED_LIST"
if [[ "${#SELECTED_CSVS[@]}" -eq 0 ]]; then
  echo "ERROR: deterministic selection produced no CSVs" >&2
  exit 1
fi

printf "csv\trun_dir\texit_code\n" > "$RUNS_TSV"

echo "[INFO] selected_csv_count=${#SELECTED_CSVS[@]}"
echo "[INFO] selected_list=$SELECTED_LIST"
echo "[INFO] runs_tsv=$RUNS_TSV"

for csv in "${SELECTED_CSVS[@]}"; do
  echo "[INFO] running ops_cycle for: $csv"
  TMP_OUT="$(mktemp)"
  set +e
  python3 -B -m tools.ops_cycle \
    --mode B \
    --input "$csv" \
    --candidates "$CANDIDATES" \
    --loop "$LOOP_N" \
    --stability-slice "$STABILITY_SLICE" \
    --no-progress-k "$NO_PROGRESS_K" | tee "$TMP_OUT"
  status=$?
  set -e

  run_dir="$(grep -E "\[INFO\] loop artifacts root:" "$TMP_OUT" | tail -1 | sed 's/.*root: //')"
  rm -f "$TMP_OUT"

  if [[ -z "${run_dir:-}" ]]; then
    run_dir="(not_found)"
  fi
  printf "%s\t%s\t%s\n" "$csv" "$run_dir" "$status" >> "$RUNS_TSV"
done

echo "[INFO] done"
echo "[INFO] manifest: $RUNS_TSV"

echo "$RUNS_TSV"
