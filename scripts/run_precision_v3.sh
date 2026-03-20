#!/bin/bash
set -euo pipefail

# ==============================================================================
# 🎯 精选12板块强化学习实验 (Precision V3)
# ==============================================================================
# 筛选标准：
# 1. 组内价格相关系数 ≥ 0.45 (走势高度同步)
# 2. 近3个月涨幅 < 30%     (估值未透支，投资机会仍在)
# 3. 板块逻辑清晰，分组规模适中 (<150只)
# ==============================================================================

cd "$(dirname "$0")/.."
source scripts/_env.sh
setup_project_env duckdb numpy pandas torch sklearn

MARKET_DUCKDB="${MARKET_DUCKDB:-data/market.duckdb}"
INDEX_DUCKDB="${INDEX_DUCKDB:-data/index_market.duckdb}"

HORIZON=1
CONTEXT_LEN=30
TRAIN_DAYS=300
TEST_DAYS=120
SAMPLE_SIZE=20
FEATURE_SET="full"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="data/tasks/precision_v3_${TIMESTAMP}"
mkdir -p "${OUTPUT_DIR}"

# 精选12组：高相关 × 未过涨
GROUP_LIST=(
  "ind_证券"       # 相关系数 0.73，近期回调 -5%，政策催化等待
  "ind_股份制银行"  # 相关系数 0.64，全年横盘，估值低
  "ind_白酒"       # 相关系数 0.60，1年跌19%，底部机会
  "ind_显示器件"    # 相关系数 0.50，全年平盘，面板周期底部
  "ind_风力发电"    # 相关系数 0.44，1年仅+5%，新能源洼地
  "ind_软件开发"    # 相关系数 0.56，信创加速，涨幅合理
  "ind_电池"       # 相关系数 0.45，近期回调 -9%，再上车机会
  "ind_电池化学品"  # 相关系数 0.54，新能源材料，近期稳定
  "con_人工智能"   # 相关系数 0.49，1年涨幅-12%，主题未到高点
  "con_比亚迪链"   # 相关系数 0.45，Baseline已验证高胜率
  "con_华为概念"   # 相关系数 0.42，Baseline已验证高胜率
  "ind_影视院线"   # 相关系数 0.47，消费复苏，小而纯
)

TOTAL=${#GROUP_LIST[@]}

echo "=========================================================================="
echo "🎯 精选12板块·强化学习实验 (Precision V3)"
echo "--------------------------------------------------------------------------"
echo "筛选逻辑: 高相关性 + 近期未过涨 (投资机会仍存)"
echo "训练深度: ${TRAIN_DAYS}天 | 回测窗口: ${TEST_DAYS}天 | 每组采样: ${SAMPLE_SIZE}只"
echo "输出路径: ${OUTPUT_DIR}"
echo "=========================================================================="

for i in "${!GROUP_LIST[@]}"; do
  group="${GROUP_LIST[$i]}"
  COUNT=$((i + 1))
  echo ""
  echo ">>> [${COUNT}/${TOTAL}] 攻坚板块: ${group} ..."

  "${PYTHON_BIN}" -u -m timesfm_cn_forecast.run_group_eval \
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
    --min-days 500 \
    --output-dir "${OUTPUT_DIR}" || {
      echo "⚠️  ${group} 运行异常，已跳过"
    }
done

echo ""
echo "=========================================================================="
echo "✅ 精选实验全部完成！"
echo "结果保存至: ${OUTPUT_DIR}"
echo ""
echo "📊 快速汇总: "
"${PYTHON_BIN}" -u -c "
import pandas as pd
from pathlib import Path
results = []
for csv in Path('${OUTPUT_DIR}').rglob('*.csv'):
    try:
        df = pd.read_csv(csv)
        ok = df[df['status']=='ok']
        if ok.empty: continue
        results.append({'板块': csv.parent.name, 
                        '胜率': round(ok['hitrate'].mean(), 1),
                        '样本数': len(ok)})
    except: pass
if results:
    rdf = pd.DataFrame(results).sort_values('胜率', ascending=False)
    print(rdf.to_string(index=False))
"
echo "=========================================================================="
