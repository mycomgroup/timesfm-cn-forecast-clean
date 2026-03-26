#!/bin/bash
set -euo pipefail

# 只运行行业和概念组的评估脚本

cd "$(dirname "$0")/.."
source "$(dirname "$0")/_env.sh"
setup_project_env duckdb numpy pandas torch sklearn

MARKET_DUCKDB="${MARKET_DUCKDB:-data/market.duckdb}"
INDEX_DUCKDB="${INDEX_DUCKDB:-data/index_market.duckdb}"

OUTPUT_DIR="${OUTPUT_DIR:-data/tasks/industry_concept_eval}"
mkdir -p "${OUTPUT_DIR}"

FEATURE_SET="${FEATURE_SET:-full}"
TRAIN_DAYS="${TRAIN_DAYS:-60}"
HORIZON="${HORIZON:-1}"
CONTEXT_LEN="${CONTEXT_LEN:-60}"
TEST_DAYS="${TEST_DAYS:-20}"
MIN_DAYS="${MIN_DAYS:-1000}"

SKIP_EXISTING="${SKIP_EXISTING:-1}"

echo ">>> 开始筛选行业和概念组..."

# 获取所有组
if ! GROUP_LIST="$(
  INDEX_DUCKDB="${INDEX_DUCKDB}" "${PYTHON_BIN}" - <<'PY'
import os
import sys
from pathlib import Path

root = Path.cwd()
src = root / "src"
sys.path.insert(0, str(src))

from timesfm_cn_forecast.universe.storage import list_all_symbols

duckdb_path = os.environ.get("INDEX_DUCKDB") or str(root / "data" / "index_market.duckdb")
df = list_all_symbols(duckdb_path)

# 筛选行业组 (ind_) 和概念组 (con_)
symbols = df["index_symbol"].astype(str).tolist()
filtered = [s for s in symbols if s.startswith('ind_') or s.startswith('con_')]
print("\n".join(filtered))
PY
)"; then
  echo "ERROR: Failed to discover groups from ${INDEX_DUCKDB}." >&2
  exit 1
fi

# 转换成数组
GROUP_ARRAY=()
while IFS= read -r line; do
  [[ -n "$line" ]] && GROUP_ARRAY+=("$line")
done <<< "$GROUP_LIST"

TOTAL_GROUPS=${#GROUP_ARRAY[@]}
if [ "${TOTAL_GROUPS}" -eq 0 ]; then
  echo "ERROR: No industry or concept groups found in ${INDEX_DUCKDB}." >&2
  exit 1
fi

echo ">>> 找到 ${TOTAL_GROUPS} 个行业和概念组要评估。"

COMPLETED=0
SKIPPED=0

for i in "${!GROUP_ARRAY[@]}"; do
  group="${GROUP_ARRAY[$i]}"
  IDX=$((i + 1))
  
  # 检查是否已完成（有 results.csv 文件）
  if [ "$SKIP_EXISTING" = "1" ] && [ -f "${OUTPUT_DIR}/${group}/results.csv" ]; then
    echo "[${IDX}/${TOTAL_GROUPS}] 跳过 ${group} (已完成)"
    ((SKIPPED++)) || true
    continue
  fi
  
  # 原子性创建目录作为简单锁
  mkdir -p "${OUTPUT_DIR}/${group}"
  
  echo "[${IDX}/${TOTAL_GROUPS}] 运行组：${group}"
  if MARKET_DUCKDB="${MARKET_DUCKDB}" \
    INDEX_DUCKDB="${INDEX_DUCKDB}" \
    OUTPUT_DIR="${OUTPUT_DIR}" \
    START_DATE="${START_DATE:-}" \
    END_DATE="${END_DATE:-}" \
    TRAIN_END="${TRAIN_END:-}" \
    TEST_START="${TEST_START:-}" \
    TEST_END="${TEST_END:-}" \
    CONTEXT_LENGTHS="${CONTEXT_LENGTHS:-}" \
    bash scripts/run_one_group_eval.sh \
      "${group}" \
      "${FEATURE_SET}" \
      "${TRAIN_DAYS}" \
      "${HORIZON}" \
      "${CONTEXT_LEN}" \
      "${TEST_DAYS}" \
      "${MIN_DAYS}"
  then
    ((COMPLETED++)) || true
  else
    rc=$?
    echo "WARNING: group ${group} failed with exit code ${rc}; continuing." >&2
    # 失败时删除空目录
    rmdir "${OUTPUT_DIR}/${group}" 2>/dev/null || true
  fi
done

echo ""
echo "=== 执行完成 ==="
echo "总计：${TOTAL_GROUPS} 组"
echo "成功：${COMPLETED} 组"
echo "跳过：${SKIPPED} 组"
echo "输出目录：${OUTPUT_DIR}"
