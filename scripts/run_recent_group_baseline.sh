#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
source "$(dirname "$0")/_env.sh"
setup_project_env duckdb numpy pandas torch sklearn

MARKET_DUCKDB="${MARKET_DUCKDB:-data/market.duckdb}"
INDEX_DUCKDB="${INDEX_DUCKDB:-data/index_market.duckdb}"

HORIZON="${HORIZON:-1}"
CONTEXT_LENGTHS="${CONTEXT_LENGTHS:-30,60}"
SAMPLE_SIZE="${SAMPLE_SIZE:-10}"
MAX_GROUPS="${MAX_GROUPS:-30}"
RECENT_WINDOWS="${RECENT_WINDOWS:-20,40,60}"
TRAIN_END="${TRAIN_END:-2025-12-31}"
TEST_START="${TEST_START:-2026-01-01}"
TEST_END="${TEST_END:-2026-03-10}"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="data/tasks/recent_group_baseline_${TIMESTAMP}"
mkdir -p "${OUTPUT_DIR}"

MAX_GROUPS="${MAX_GROUPS}" INDEX_DUCKDB="${INDEX_DUCKDB}" GROUP_LIST=$("${PYTHON_BIN}" - <<'PY'
import os
import sys
from pathlib import Path

root = Path.cwd()
sys.path.insert(0, str(root / "src"))
from timesfm_cn_forecast.universe.storage import list_all_symbols

df = list_all_symbols(os.environ["INDEX_DUCKDB"])
groups = df["index_symbol"].tolist()[: int(os.environ.get("MAX_GROUPS", "30"))]
print(" ".join(groups))
PY
)

for group in $GROUP_LIST; do
  echo ">>> recent baseline: ${group}"
  "${PYTHON_BIN}" -m timesfm_cn_forecast.run_group_baseline \
    --group "${group}" \
    --market-duckdb "${MARKET_DUCKDB}" \
    --index-duckdb "${INDEX_DUCKDB}" \
    --horizon "${HORIZON}" \
    --context-lengths "${CONTEXT_LENGTHS}" \
    --sample-size "${SAMPLE_SIZE}" \
    --test-days 60 \
    --train-end "${TRAIN_END}" \
    --test-start "${TEST_START}" \
    --test-end "${TEST_END}" \
    --rolling-windows "${RECENT_WINDOWS}" \
    --output-dir "${OUTPUT_DIR}"
done

echo "Done. Output: ${OUTPUT_DIR}"
