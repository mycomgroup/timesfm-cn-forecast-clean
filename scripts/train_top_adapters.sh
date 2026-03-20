#!/bin/bash
set -euo pipefail

# Finetuning the Top 3 Groups (Patching Phase)
# 
# This script trains a specialized adapter (patch) for each of the top 
# three groups identified in the baseline scan.
#
# Parameters:
# - Context Length: 30 (to match baseline)
# - Train Days: 200 (sufficient history to learn technical patterns)
# - Test Days: 120 (rolling backtest to compare with baseline)
# - Feature Set: full (OHLCV + Technical indicators)

cd "$(dirname "$0")/.."
source scripts/_env.sh
setup_project_env duckdb numpy pandas torch sklearn

MARKET_DUCKDB="${MARKET_DUCKDB:-data/market.duckdb}"
INDEX_DUCKDB="${INDEX_DUCKDB:-data/index_market.duckdb}"

HORIZON=1
CONTEXT_LEN=30
TRAIN_DAYS=200
TEST_DAYS=120
SAMPLE_SIZE=20
FEATURE_SET="full"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="data/tasks/finetune_top3_${TIMESTAMP}"
mkdir -p "${OUTPUT_DIR}"

TOP_GROUPS=("ind_白酒" "con_华为概念" "con_比亚迪链")

echo "=========================================================================="
echo "🚀 开始为前三强板块训练专用补丁 (Adapter)..."
echo "目标板块: ${TOP_GROUPS[*]}"
echo "训练时长: ${TRAIN_DAYS} 天 | 特征集: ${FEATURE_SET}"
echo "验证周期: ${TEST_DAYS} 日滚动回测"
echo "=========================================================================="

for group in "${TOP_GROUPS[@]}"; do
  echo ">>> [Finetune] 正在强化训练板块: ${group} ..."
  "${PYTHON_BIN}" -m timesfm_cn_forecast.run_group_eval \
    --group "${group}" \
    --market-duckdb "${MARKET_DUCKDB}" \
    --index-duckdb "${INDEX_DUCKDB}" \
    --feature-set "${FEATURE_SET}" \
    --train-days "${TRAIN_DAYS}" \
    --horizon "${HORIZON}" \
    --context-len "${CONTEXT_LEN}" \
    --context-lengths "${CONTEXT_LEN}" \
    --test-days "${TEST_DAYS}" \
    --sample-size "${SAMPLE_SIZE}" \
    --output-dir "${OUTPUT_DIR}"
done

echo "=========================================================================="
echo "🎉 前三强板块强化训练完成！"
echo "所有结果及 Adapter 已保存至: ${OUTPUT_DIR}"
echo "接下来你可以运行汇总脚本将 Patched 结果与 Baseline 进行对比。"
echo "=========================================================================="
