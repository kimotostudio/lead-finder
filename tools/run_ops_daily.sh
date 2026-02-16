#!/usr/bin/env bash
set -euo pipefail

cd "$HOME/projects/lead-finder"

# venv
source .venv/bin/activate

# 入力CSVは「最新の leads_*.csv を使う」
LATEST_CSV="$(ls -t web_app/output/leads_*.csv 2>/dev/null | head -n 1)"

if [ -z "${LATEST_CSV}" ]; then
  echo "[ERROR] No leads_*.csv found in web_app/output"
  exit 1
fi

echo "=== OPS DAILY START ==="
date
echo "INPUT=${LATEST_CSV}"

python3 -m tools.ops_cycle --slice 200 --input "${LATEST_CSV}"

echo "=== OPS DAILY END ==="
date

