#!/bin/bash
set -euo pipefail

# ==============================================================================
# 🧬 共振相关性聚类实验矩阵 (Resonance V2)
# ==============================================================================
# 逻辑：使用 AI 自动发现的“价格共振簇”进行训练，不再受行业划分局限。
# ==============================================================================

cd "$(dirname "$0")/.."
export PYTHONPATH=src
export PATH=/opt/anaconda3/bin:$PATH

MARKET_DUCKDB="data/market.duckdb"
INDEX_DUCKDB="data/index_market.duckdb"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="data/tasks/resonance_v2_${TIMESTAMP}"
mkdir -p "${OUTPUT_DIR}"

GROUP_LIST=(
  "resonance_sh688095"
  "resonance_sz300057"
  "resonance_sh688695"
  "resonance_sh600519"
  "resonance_sz300656"
  "resonance_sz300292"
  "resonance_sh688152"
  "resonance_sz301327"
)

echo "=========================================================================="
echo "🧬 共振相关性实验启动 (V2)"
echo "--------------------------------------------------------------------------"
echo "输出路径: ${OUTPUT_DIR}"
echo "=========================================================================="

for group in "${GROUP_LIST[@]}"; do
  echo ">>> 正在处理共振组: ${group} ..."
  python -u -m timesfm_cn_forecast.run_group_eval \
    --group "${group}" \
    --market-duckdb "${MARKET_DUCKDB}" \
    --index-duckdb "${INDEX_DUCKDB}" \
    --feature-set full \
    --train-days 300 \
    --horizon 1 \
    --context-len 30 \
    --context-lengths 30 \
    --test-days 120 \
    --output-dir "${OUTPUT_DIR}" || echo "⚠️  ${group} 失败"
done

echo "✅ 共振实验完成！"
