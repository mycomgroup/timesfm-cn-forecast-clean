#!/选/bin/bash
set -euo pipefail

# 实验矩阵自动化脚本
# 针对 small_fengzhi 股票池执行 16 组排列组合实验

cd "$(dirname "$0")/.."

GROUP="${GROUP:-small}"
FEATURE_SETS=("basic" "technical" "structural" "full")

# 场景定义: (TRAIN_DAYS HORIZON CONTEXT_LEN)
SCENARIOS=(
    "60 1 64"    # 极短线敏捷型
    "180 1 128"  # 短线波段型
    "500 5 256"  # 中线趋势型
    "250 3 128"  # 折中型
)

# 随机采样验证数量 (提升实验速度)
SAMPLE_SIZE="${SAMPLE_SIZE:-100}"

export PATH=/opt/anaconda3/bin:$PATH
export PYTHONPATH=src

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
ROOT_OUTPUT_DIR="data/tasks/matrix_${GROUP}_${TIMESTAMP}"
mkdir -p "${ROOT_OUTPUT_DIR}"

echo "开始执行全量实验矩阵 (分组: ${GROUP}, 时间戳: ${TIMESTAMP})"
echo "采样验证数量: ${SAMPLE_SIZE}"
echo "结果将汇总至目录: ${ROOT_OUTPUT_DIR}"

for FS in "${FEATURE_SETS[@]}"; do
    for SCENARIO in "${SCENARIOS[@]}"; do
        read -r TD H CL <<< "$SCENARIO"
        
        TASK_NAME="FS_${FS}_TD_${TD}_H_${H}_CL_${CL}"
        
        echo "----------------------------------------------------------------"
        echo "正在运行场景: ${TASK_NAME}"
        echo "  特征集: ${FS} | 训练天数: ${TD} | 预测步长: ${H} | 上下文: ${CL}"
        
        # 将所有场景的结果都保存在同一个 ROOT_OUTPUT_DIR 目录下
        # run_group_eval.py 已更新，会生成带参数的文件名
        OUTPUT_DIR="${ROOT_OUTPUT_DIR}" FEATURE_SET="${FS}" TRAIN_DAYS="${TD}" HORIZON="${H}" CONTEXT_LEN="${CL}" \
        bash scripts/run_one_group_eval.sh "${GROUP}" "${FS}" "${TD}" "${H}" "${CL}" 20 1000 --sample-size "${SAMPLE_SIZE}"
        
        echo "场景 ${TASK_NAME} 完成。"
    done
done
done

echo "----------------------------------------------------------------"
echo "所有 16 组实验已完成。"
echo "请运行 python -m timesfm_cn_forecast.analyze_matrix_results --dir ${ROOT_OUTPUT_DIR} 进行汇总。"
