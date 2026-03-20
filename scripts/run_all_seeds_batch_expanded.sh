#!/bin/bash
# =============================================================================
# 扩容种子批跑脚本 (Batch Evaluation for Expanded Seeds)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}/.."
source scripts/_env.sh
setup_project_env

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BATCH_FILE="/tmp/seed_batch_expanded_${TIMESTAMP}.log"

# 定义 15 个种子
SEEDS=(
  "301106" "300566" "000596" "300793" "688111"
  "300110" "300838" "301218" "301332" "600526"
  "601916" "300369" "000862" "002903" "300438"
)

echo ">>> Starting batch evaluation for ${#SEEDS[@]} expanded seeds..."
echo ">>> Log file: ${BATCH_FILE}"

export TRAIN_END=2025-12-31
export TEST_START=2026-01-01
export TEST_END=2026-03-10

SUCCESS_COUNT=0
FAIL_COUNT=0
FAILED_SEEDS=()

for seed in "${SEEDS[@]}"; do
  echo "===== [$(date +"%H:%M:%S")] Starting seed: ${seed} =====" | tee -a "${BATCH_FILE}"
  
  # 使用 expanded 目录下的定义 (如果存在)
  DEF_PATH="data/group_definitions_expanded/seed_${seed}.json"
  if [ ! -f "${DEF_PATH}" ]; then
      DEF_PATH="data/group_definitions/seed_${seed}.json"
  fi
  
  echo ">>> Using definition: ${DEF_PATH}" | tee -a "${BATCH_FILE}"
  
  # 运行单种子评估流程
  # 失败时记录并继续下一个，避免整批异常中断
  if bash scripts/run_seed_group_eval.sh "${seed}" "${DEF_PATH}" >> "${BATCH_FILE}" 2>&1; then
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    echo "===== [$(date +"%H:%M:%S")] Finished seed: ${seed} =====" | tee -a "${BATCH_FILE}"
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED_SEEDS+=("${seed}")
    echo "===== [$(date +"%H:%M:%S")] Failed seed: ${seed}, continuing... =====" | tee -a "${BATCH_FILE}"
  fi
done

echo ">>> ALL EXPANDED SEEDS DONE <<<" | tee -a "${BATCH_FILE}"
echo ">>> Success: ${SUCCESS_COUNT}, Failed: ${FAIL_COUNT}" | tee -a "${BATCH_FILE}"
if [ "${FAIL_COUNT}" -gt 0 ]; then
  echo ">>> Failed seeds: ${FAILED_SEEDS[*]}" | tee -a "${BATCH_FILE}"
fi
