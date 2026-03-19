#!/bin/bash
set -euo pipefail

# Examples:
# 1) Basic run (group only):
#    bash scripts/run_one_group_eval.sh CYBZ
# 2) With explicit params:
#    bash scripts/run_one_group_eval.sh ind_消费电子 full 60 1 60 20 1000
# 3) With env overrides:
#    MARKET_DUCKDB=data/market.duckdb INDEX_DUCKDB=data/index_market.duckdb \
#    OUTPUT_DIR=data/research FEATURE_SET=full TRAIN_DAYS=60 HORIZON=1 \
#    CONTEXT_LEN=60 TEST_DAYS=20 MIN_DAYS=1000 \
#    START_DATE=2015-01-01 END_DATE=2025-12-31 CONTEXT_LENGTHS=30,60,90 \
#    bash scripts/run_one_group_eval.sh CYBZ

cd "$(dirname "$0")/.."
source "$(dirname "$0")/_env.sh"
setup_project_env duckdb numpy pandas torch sklearn

# 获取位置参数
GROUP="${1:-}"
if [ -z "$GROUP" ]; then
  echo "Usage: $0 <group> [feature_set] [train_days] [horizon] [context_len] [test_days] [min_days] [extra_args...]"
  exit 1
fi
shift

FEATURE_SET="${1:-full}"
[ $# -gt 0 ] && shift || true

TRAIN_DAYS="${1:-60}"
[ $# -gt 0 ] && shift || true

HORIZON="${1:-1}"
[ $# -gt 0 ] && shift || true

CONTEXT_LEN="${1:-60}"
[ $# -gt 0 ] && shift || true

TEST_DAYS="${1:-20}"
[ $# -gt 0 ] && shift || true

MIN_DAYS="${1:-1000}"
[ $# -gt 0 ] && shift || true

MARKET_DUCKDB="${MARKET_DUCKDB:-data/market.duckdb}"
INDEX_DUCKDB="${INDEX_DUCKDB:-data/index_market.duckdb}"

if [ -z "${OUTPUT_DIR:-}" ] || [ "${OUTPUT_DIR:-}" == "data/research" ]; then
  TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
  TASK_DIR="data/tasks/eval_group_${GROUP}_${TIMESTAMP}"
  OUTPUT_DIR="${TASK_DIR}/groups"
else
  # 如果外部指定了 OUTPUT_DIR，我们直接使用它，不再包一层 groups
  OUTPUT_DIR="${OUTPUT_DIR}"
fi

mkdir -p "${OUTPUT_DIR}"

"${PYTHON_BIN}" -m timesfm_cn_forecast.run_group_eval \
  --group "${GROUP}" \
  --market-duckdb "${MARKET_DUCKDB}" \
  --index-duckdb "${INDEX_DUCKDB}" \
  --feature-set "${FEATURE_SET}" \
  --train-days "${TRAIN_DAYS}" \
  --horizon "${HORIZON}" \
  --context-len "${CONTEXT_LEN}" \
  --test-days "${TEST_DAYS}" \
  --min-days "${MIN_DAYS}" \
  --output-dir "${OUTPUT_DIR}" \
  ${START_DATE:+--start "$START_DATE"} \
  ${END_DATE:+--end "$END_DATE"} \
  ${CONTEXT_LENGTHS:+--context-lengths "$CONTEXT_LENGTHS"} \
  "$@"
