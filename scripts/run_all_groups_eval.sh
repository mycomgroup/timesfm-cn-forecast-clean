#!/bin/bash
set -euo pipefail

# Examples:
# 1) Basic run (all groups, skip existing, auto-analyze):
#    bash scripts/run_all_groups_eval.sh
# 2) Change core params:
#    FEATURE_SET=full TRAIN_DAYS=60 HORIZON=1 CONTEXT_LEN=60 TEST_DAYS=20 MIN_DAYS=1000 \
#    bash scripts/run_all_groups_eval.sh
# 3) With date filter and output dir:
#    OUTPUT_DIR=data/research START_DATE=2015-01-01 END_DATE=2025-12-31 \
#    CONTEXT_LENGTHS=30,60,90 \
#    bash scripts/run_all_groups_eval.sh
# 4) Re-run all groups (do not skip existing) and skip analyze:
#    SKIP_EXISTING=0 ANALYZE=0 bash scripts/run_all_groups_eval.sh

cd "$(dirname "$0")/.."
source "$(dirname "$0")/_env.sh"
setup_project_env duckdb numpy pandas torch sklearn

MARKET_DUCKDB="${MARKET_DUCKDB:-data/market.duckdb}"
INDEX_DUCKDB="${INDEX_DUCKDB:-data/index_market.duckdb}"

if [ -z "${OUTPUT_DIR:-}" ]; then
  TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
  TASK_DIR="data/tasks/eval_all_groups_${TIMESTAMP}"
  OUTPUT_DIR="${TASK_DIR}/groups"
fi

mkdir -p "${OUTPUT_DIR}"

FEATURE_SET="${FEATURE_SET:-full}"
TRAIN_DAYS="${TRAIN_DAYS:-60}"
HORIZON="${HORIZON:-1}"
CONTEXT_LEN="${CONTEXT_LEN:-60}"
TEST_DAYS="${TEST_DAYS:-20}"
MIN_DAYS="${MIN_DAYS:-1000}"

SKIP_EXISTING="${SKIP_EXISTING:-1}"
ANALYZE="${ANALYZE:-1}"

if ! GROUP_LIST="$(
  INDEX_DUCKDB="${INDEX_DUCKDB}" "${PYTHON_BIN}" - <<'PY'
import os
import sys
from pathlib import Path

root = Path.cwd()
src = root / "src"
sys.path.insert(0, str(src))

from timesfm_cn_forecast.universe.storage import list_all_symbols

duckdb_path = os.environ.get("INDEX_DUCKDB") or str(root / "data" / "index_market.duckdb")
df = list_all_symbols(duckdb_path)
print("\n".join(df["index_symbol"].astype(str).tolist()))
PY
)"; then
  echo "ERROR: Failed to discover groups from ${INDEX_DUCKDB}." >&2
  exit 1
fi

# 转换成数组，避免空格分词问题
GROUP_ARRAY=()
while IFS= read -r line; do
  [[ -n "$line" ]] && GROUP_ARRAY+=("$line")
done <<< "$GROUP_LIST"

TOTAL_GROUPS=${#GROUP_ARRAY[@]}
if [ "${TOTAL_GROUPS}" -eq 0 ]; then
  echo "ERROR: No groups found in ${INDEX_DUCKDB}." >&2
  exit 1
fi
echo ">>> Found ${TOTAL_GROUPS} groups to evaluate."

for i in "${!GROUP_ARRAY[@]}"; do
  group="${GROUP_ARRAY[$i]}"
  IDX=$((i + 1))
  
  if [ "$SKIP_EXISTING" = "1" ] && [ -d "${OUTPUT_DIR}/${group}" ]; then
    echo "[${IDX}/${TOTAL_GROUPS}] Skipping ${group} (directory exists, possibly running or finished)"
    continue
  fi
  
  # 原子性创建目录作为简单锁
  mkdir -p "${OUTPUT_DIR}/${group}"
  
  echo "[${IDX}/${TOTAL_GROUPS}] Running group: ${group}"
  if MARKET_DUCKDB="${MARKET_DUCKDB}" \
    INDEX_DUCKDB="${INDEX_DUCKDB}" \
    OUTPUT_DIR="${OUTPUT_DIR}" \
    START_DATE="${START_DATE:-}" \
    END_DATE="${END_DATE:-}" \
    TRAIN_END="${TRAIN_END:-}" \
    TEST_START="${TEST_START:-}" \
    TEST_END="${TEST_END:-}" \
    CONTEXT_LENGTHS="${CONTEXT_LENGTHS:-}" \
    bash scripts/run_one_group_eval.sh \
      "${group}" \
      "${FEATURE_SET}" \
      "${TRAIN_DAYS}" \
      "${HORIZON}" \
      "${CONTEXT_LEN}" \
      "${TEST_DAYS}" \
      "${MIN_DAYS}"
  then
    :
  else
    rc=$?
    echo "WARNING: group ${group} failed with exit code ${rc}; continuing." >&2
  fi
done

if [ "$ANALYZE" = "1" ]; then
  if ! "${PYTHON_BIN}" -m timesfm_cn_forecast.analyze_group_results --input-dir "${OUTPUT_DIR}"; then
    echo "WARNING: analyze_group_results failed." >&2
  fi
fi
