#!/bin/bash
set -euo pipefail

# ==============================================================================
# 🚀 极致规模：全市场全版块强化学习与回归回测 (Massive Matrix V2)
# ==============================================================================
# 核心提升：
# 1. 500天训练深度：充分捕捉不同市场周期下的技术形态。
# 2. 250天回测深度：整整一年的历史滚动模拟，排除节假日和极端天气干扰。
# 3. 每组30只样板：更具统计学意义的胜率分布。
# 4. 归一化特征：解决价格跨度带来的 Adapter 不自洽问题。
# ==============================================================================

cd "$(dirname "$0")/.."
export PATH=/opt/anaconda3/bin:$PATH
export PYTHONPATH=src

MARKET_DUCKDB="${MARKET_DUCKDB:-data/market.duckdb}"
INDEX_DUCKDB="${INDEX_DUCKDB:-data/index_market.duckdb}"

HORIZON=1
CONTEXT_LEN=30
TRAIN_DAYS=500
TEST_DAYS=250
SAMPLE_SIZE=30
FEATURE_SET="full"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="data/tasks/massive_matrix_${TIMESTAMP}"
mkdir -p "${OUTPUT_DIR}"

# 动态获取所有可用的组名
GROUP_LIST=$(PYTHONPATH=src python -c "from timesfm_cn_forecast.universe.fetcher import INDEX_MAP; print(' '.join(INDEX_MAP.keys()))")

echo "=========================================================================="
echo "🌟 开启全市场‘地毯式’强化学习实验矩阵 (V2)"
echo "--------------------------------------------------------------------------"
echo "数据源: ${MARKET_DUCKDB}"
echo "总分组: $(echo $GROUP_LIST | wc -w) 个版块"
echo "训练深度: ${TRAIN_DAYS} 天 | 回测深度: ${TEST_DAYS} 天 (约1年)"
echo "特征配置: ${FEATURE_SET} (已归一化)"
echo "保存路径: ${OUTPUT_DIR}"
echo "=========================================================================="

COUNT=0
TOTAL=$(echo $GROUP_LIST | wc -w)

for group in $GROUP_LIST; do
  COUNT=$((COUNT + 1))
  echo ">>> [${COUNT}/${TOTAL}] 正在攻坚版块: ${group} ..."
  
  # 运行强化训练 + 回测
  # 我们在这里加上一个 --start 参数限制，保证数据范围一致性 (如果需要)
  python -u -m timesfm_cn_forecast.run_group_eval \
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
    --output-dir "${OUTPUT_DIR}" || {
      echo "⚠️  版块 ${group} 运行出现异常，已跳过。"
    }
done

echo "=========================================================================="
echo "✅ 史实级全市场巡检圆满结束！"
echo "你可以运行分析脚本对 ${OUTPUT_DIR} 下的数十个 CSV 进行终极胜率汇总。"
echo "=========================================================================="
