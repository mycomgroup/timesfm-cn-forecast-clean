#!/bin/bash
set -euo pipefail

# Scripts to run zero-shot baseline evaluation across all groups 
# using local DuckDB data. This skips the adapter training entirely.
#
# Defaults: Evaluate 30 days of context, predicting 1 day ahead, 
# testing on the last 5 days of data, sampling 10 stocks per group.
#
# Usage: bash scripts/run_baseline_all_groups.sh

cd "$(dirname "$0")/.."

export PATH=/opt/anaconda3/bin:$PATH
export PYTHONPATH=src

MARKET_DUCKDB="${MARKET_DUCKDB:-data/market.duckdb}"
INDEX_DUCKDB="${INDEX_DUCKDB:-data/index_market.duckdb}"

HORIZON="${HORIZON:-1}"
CONTEXT_LENGTHS="${CONTEXT_LENGTHS:-30}"
TEST_DAYS="${TEST_DAYS:-120}"
SAMPLE_SIZE="${SAMPLE_SIZE:-10}"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="data/tasks/baseline_all_groups_${TIMESTAMP}"
mkdir -p "${OUTPUT_DIR}"

echo "=========================================================================="
echo "🚀 启动全市场 Baseline (原始模型, 无 Patch) 极速扫描评估"
echo "上下文长度: ${CONTEXT_LENGTHS} | 预测步长: ${HORIZON} | 回溯天数: ${TEST_DAYS}"
echo "每个板块采样股票数: ${SAMPLE_SIZE}"
echo "输出目录: ${OUTPUT_DIR}"
echo "=========================================================================="

GROUP_LIST=$(
  INDEX_DUCKDB="${INDEX_DUCKDB}" python - <<'PY'
import os
import sys
from pathlib import Path
root = Path.cwd()
src = root / "src"
sys.path.insert(0, str(src))
from timesfm_cn_forecast.universe.storage import list_all_symbols
duckdb_path = os.environ.get("INDEX_DUCKDB") or str(root / "data" / "index_market.duckdb")
df = list_all_symbols(duckdb_path)
print(" ".join(df["index_symbol"].tolist()))
PY
)

for group in $GROUP_LIST; do
  echo ">>> 开始评估板块: ${group}"
  python -m timesfm_cn_forecast.run_group_baseline \
    --group "${group}" \
    --market-duckdb "${MARKET_DUCKDB}" \
    --index-duckdb "${INDEX_DUCKDB}" \
    --horizon "${HORIZON}" \
    --context-lengths "${CONTEXT_LENGTHS}" \
    --test-days "${TEST_DAYS}" \
    --sample-size "${SAMPLE_SIZE}" \
    --output-dir "${OUTPUT_DIR}"
done

echo "=========================================================================="
echo "🎉 全市场 Baseline 扫描完成！"
echo "所有各板块的采样结果已保存在: ${OUTPUT_DIR}"
echo "你可以遍历寻找 Hit Rate 高的板块进行后续深耕。"
echo "=========================================================================="
