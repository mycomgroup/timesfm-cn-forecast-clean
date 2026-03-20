#!/bin/bash
set -euo pipefail

# 极速版: 发现哪些组配合哪些因子预测效果最好 (Fast Track)
# 核心思路：
# 1. 不跑全量市场，只抽取3个代表性概念/行业作为探索对象。
# 2. 从每个组里只随机挑“5只”股票作为样本 (极速采样)。
# 3. 缩减训练时间窗口，用最小数据量验证因子的预测能力。

cd "$(dirname "$0")/.."
source scripts/_env.sh
setup_project_env

# 1. 挑选非常有代表性、波动性强的三个组
# 比如：电池（新能源代表），半导体（科技代表），银行（稳定代表）
TARGET_GROUPS=("SW_Battery" "SW_Semiconductor" "SW_Bank")

# 2. 精简特征集和场景
FEATURE_SETS=("basic" "full") # 只看基础特征 vs 加满因子的全特征的区别

# 极简场景：180天数据，128天上下文，预测后1天
TD=180
H=1
CL=128

# 极速采样大小：每个组只验证 5 支票以评估整体 HitRate
SAMPLE_SIZE=5

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
ROOT_OUTPUT_DIR="data/tasks/fast_track_${TIMESTAMP}"
mkdir -p "${ROOT_OUTPUT_DIR}"

echo "========================================================"
echo "🚀 启动极速因子与股票组探索发现 (Fast Track)"
echo "时间戳: ${TIMESTAMP}"
echo "目标组别: ${TARGET_GROUPS[*]}"
echo "特征对比: ${FEATURE_SETS[*]}"
echo "========================================================"

for GROUP in "${TARGET_GROUPS[@]}"; do
    for FS in "${FEATURE_SETS[@]}"; do
        echo "--> 正在速测板块: [${GROUP}] | 特征集: [${FS}]"
        
        # 复用 run_one_group_eval，但是强行注入极小的 SAMPLE_SIZE 参数
        # 注意: 原脚本中可能没有直接提取所有参数，这里用环境变量强行传递
        OUTPUT_DIR="${ROOT_OUTPUT_DIR}" FEATURE_SET="${FS}" TRAIN_DAYS="${TD}" HORIZON="${H}" CONTEXT_LEN="${CL}" \
        bash scripts/run_one_group_eval.sh "${GROUP}" "${FS}" "${TD}" "${H}" "${CL}" 20 1000 --sample-size "${SAMPLE_SIZE}" >/dev/null 2>&1 || true
        
        # 提取刚刚生成的 eval.log 的最后几行指标 (假设脚本执行完毕)
        LATEST_LOG_DIR=$(ls -td ${ROOT_OUTPUT_DIR}/eval_group_${GROUP}_* 2>/dev/null | head -n 1 || true)
        if [ -n "$LATEST_LOG_DIR" ] && [ -f "$LATEST_LOG_DIR/eval.log" ]; then
            echo "   🏁 结果:"
            # 过滤出包含 RMSE 和 HitRate 的关键行
            grep -E "Average RMSE|Overall Hit Rate" "$LATEST_LOG_DIR/eval.log" || echo "   暂无有效日志"
        else
            echo "   !! 执行失败或无日志生成"
        fi
        echo "--------------------------------------------------------"
    done
done

echo "🎉 极速探索完毕！详细结果保存在 ${ROOT_OUTPUT_DIR}"
echo "--------------------------------------------------------"
echo "你可以通过运行 python -m timesfm_cn_forecast.analyze_matrix_results --dir ${ROOT_OUTPUT_DIR} 获得更清晰的对比表。"
