#!/bin/bash
set -euo pipefail

# =============================================================================
# 全量行业普查评估 (The Census Task)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}/.."
source scripts/_env.sh
setup_project_env duckdb

INDEX_DUCKDB="${PROJECT_ROOT}/data/index_market.duckdb"
MARKET_DUCKDB="${PROJECT_ROOT}/data/market.duckdb"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
export OUTPUT_DIR="${PROJECT_ROOT}/data/tasks/eval_all_groups_${TIMESTAMP}/groups"
mkdir -p "${OUTPUT_DIR}"

# P0: Define fixed time windows for census to prevent leakage
export TRAIN_END="2025-12-31"
export TEST_START="2026-01-01"
export TEST_END="2026-03-10"

echo ">>> Starting Census at ${TIMESTAMP}"
echo ">>> Results will be in: ${OUTPUT_DIR}"

# 直接获取合规的分组列表 (Python 内部过滤)
mapfile -t GROUP_LIST < <("${PYTHON_BIN}" - "${INDEX_DUCKDB}" <<'PY'
import sys
import warnings
import duckdb
warnings.filterwarnings("ignore")
try:
    conn = duckdb.connect(sys.argv[1])
    res = conn.execute("SELECT DISTINCT index_symbol FROM index_constituents").fetchall()
    symbols = [r[0] for r in res]
    allowed = ("ind_", "con_", "resonance_", "vol_", "HS300", "ZZ500", "ZZ800", "CYBZ", "ZXBZ", "small", "A", "AA")
    valid = sorted([s for s in symbols if s.startswith(allowed)])
    print("\n".join(valid))
except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr)
    sys.exit(1)
PY
)

if [ "${#GROUP_LIST[@]}" -eq 0 ]; then
    echo "ERROR: Group list is empty."
    exit 1
fi

COUNT="${#GROUP_LIST[@]}"
echo ">>> Found ${COUNT} groups to evaluate."

# 迭代并逐个调用
i=0
for group in "${GROUP_LIST[@]}"; do
    i=$((i+1))
    [ -z "$group" ] && continue

    echo "=========================================="
    echo " [${i}/${COUNT}] Group: ${group}"
    echo "=========================================="

    bash scripts/run_one_group_eval.sh \
        "${group}" \
        "full" \
        "200" \
        "1" \
        "30" \
        "40" \
        "300" \
        --train-end "${TRAIN_END}" \
        --test-start "${TEST_START}" \
        --test-end "${TEST_END}" \
        >> /tmp/census_worker.log 2>&1 || echo "!! FAILED: ${group}"
done

echo ">>> CENSUS COMPLETE <<<"
