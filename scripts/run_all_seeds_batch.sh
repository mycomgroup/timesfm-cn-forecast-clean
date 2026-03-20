#!/bin/bash
set -euo pipefail

# =============================================================================
# 全量种子评估批跑
#
# Usage:
#   bash scripts/run_all_seeds_batch.sh [--skip-existing] [--dry-run]
#
# 自动扫描 data/group_definitions/seed_*.json，对每个种子运行 run_seed_group_eval.sh。
# 结果汇总到 data/tasks/seed_batch_<timestamp>/
# =============================================================================

cd "$(dirname "$0")/.."
source "$(dirname "$0")/_env.sh"
setup_project_env pandas numpy

SKIP_EXISTING=false
DRY_RUN=false
MAX_SEEDS=0  # 0 = all

for arg in "$@"; do
  case "${arg}" in
    --skip-existing) SKIP_EXISTING=true ;;
    --dry-run) DRY_RUN=true ;;
    --max-seeds=*) MAX_SEEDS="${arg#*=}" ;;
  esac
done

# ─────────────────────────────────────────────────────────────────────────────
# 扫描所有种子定义
# ─────────────────────────────────────────────────────────────────────────────
SEED_DEFS=($(ls data/group_definitions/seed_*.json 2>/dev/null || true))
if [ ${#SEED_DEFS[@]} -eq 0 ]; then
  echo "ERROR: No seed definitions found in data/group_definitions/"
  exit 1
fi

# 提取种子代码列表
SEEDS=()
for def in "${SEED_DEFS[@]}"; do
  seed_code=$(basename "${def}" .json | sed 's/^seed_//')
  SEEDS+=("${seed_code}")
done

# Apply max-seeds limit
if [ "${MAX_SEEDS}" -gt 0 ] 2>/dev/null && [ ${#SEEDS[@]} -gt "${MAX_SEEDS}" ]; then
  SEEDS=("${SEEDS[@]:0:${MAX_SEEDS}}")
fi

TOTAL=${#SEEDS[@]}
echo "=========================================="
echo " Seed Batch Evaluation"
echo " Total seeds: ${TOTAL}"
echo " Skip existing: ${SKIP_EXISTING}"
echo " Dry run: ${DRY_RUN}"
echo "=========================================="
echo ""
echo "Seeds to evaluate: ${SEEDS[*]}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 批跑目录
# ─────────────────────────────────────────────────────────────────────────────
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BATCH_DIR="data/tasks/seed_batch_${TIMESTAMP}"
mkdir -p "${BATCH_DIR}"

# 记录元信息
cat > "${BATCH_DIR}/meta.json" <<EOF
{
  "type": "seed_batch",
  "seeds": [$(printf '"%s",' "${SEEDS[@]}" | sed 's/,$//')],
  "total": ${TOTAL},
  "skip_existing": ${SKIP_EXISTING},
  "started_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "env": {
    "TRAIN_DAYS": "${TRAIN_DAYS:-200}",
    "HORIZON": "${HORIZON:-1}",
    "CONTEXT_LEN": "${CONTEXT_LEN:-30}",
    "TEST_DAYS": "${TEST_DAYS:-60}",
    "TEST_START": "${TEST_START:-2026-01-01}",
    "TEST_END": "${TEST_END:-2026-03-10}"
  }
}
EOF

# ─────────────────────────────────────────────────────────────────────────────
# 逐种子执行
# ─────────────────────────────────────────────────────────────────────────────
SUCCESS_COUNT=0
SKIP_COUNT=0
FAIL_COUNT=0
FAILED_SEEDS=()

for i in "${!SEEDS[@]}"; do
  SEED="${SEEDS[$i]}"
  IDX=$((i + 1))
  echo ""
  echo "══════════════════════════════════════════"
  echo " [${IDX}/${TOTAL}] Seed: ${SEED}"
  echo "══════════════════════════════════════════"

  # 检查是否已有结果
  if [ "${SKIP_EXISTING}" = true ]; then
    EXISTING=$(ls -d data/tasks/seed_group_eval_${SEED}_* 2>/dev/null | head -1 || true)
    if [ -n "${EXISTING}" ] && [ -f "${EXISTING}/summary/seed_group_compare.csv" ]; then
      echo "  ⏭  Skipping (existing result: ${EXISTING})"
      SKIP_COUNT=$((SKIP_COUNT + 1))
      continue
    fi
  fi

  if [ "${DRY_RUN}" = true ]; then
    echo "  🔍 [DRY-RUN] Would run: bash scripts/run_seed_group_eval.sh ${SEED}"
    continue
  fi

  START_TIME=$(date +%s)
  if bash scripts/run_seed_group_eval.sh "${SEED}"; then
    END_TIME=$(date +%s)
    ELAPSED=$(( END_TIME - START_TIME ))
    echo "  ✅ Success (${ELAPSED}s)"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    END_TIME=$(date +%s)
    ELAPSED=$(( END_TIME - START_TIME ))
    echo "  ❌ Failed (${ELAPSED}s)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED_SEEDS+=("${SEED}")
  fi
done

echo ""
echo "=========================================="
echo " Batch Complete"
echo "  ✅ Success: ${SUCCESS_COUNT}"
echo "  ⏭  Skipped: ${SKIP_COUNT}"
echo "  ❌ Failed:  ${FAIL_COUNT}"
if [ ${#FAILED_SEEDS[@]} -gt 0 ]; then
  echo "  Failed seeds: ${FAILED_SEEDS[*]}"
fi
echo "=========================================="

# ─────────────────────────────────────────────────────────────────────────────
# 汇总所有种子的最优结果
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Aggregating best seed group results..."

"${PYTHON_BIN}" - "${BATCH_DIR}" <<'PY'
import json
import sys
from pathlib import Path

import pandas as pd

batch_dir = Path(sys.argv[1])
tasks_dir = Path("data/tasks")

rows = []
for task_dir in sorted(tasks_dir.glob("seed_group_eval_*")):
    summary_path = task_dir / "summary" / "seed_group_compare.csv"
    if not summary_path.exists():
        continue
    try:
        df = pd.read_csv(summary_path, low_memory=False)
    except Exception:
        continue
    if df.empty:
        continue

    seed_col = "seed"
    if seed_col not in df.columns:
        continue

    seed = str(df[seed_col].iloc[0]).replace(".0", "").zfill(6)

    # 取最新的结果（按 task 时间戳排序后最后一个）
    best_row = df.iloc[0].to_dict()
    best_row["seed"] = seed
    best_row["source_task"] = task_dir.name
    rows.append(best_row)

if not rows:
    print("No seed evaluation results found.")
    sys.exit(0)

# 对同一 seed 取最新的那条
result_df = pd.DataFrame(rows)
result_df = result_df.drop_duplicates(subset="seed", keep="last")
result_df = result_df.sort_values(
    by=["trade_score", "hitrate", "recent20_avg_ret"],
    ascending=[False, False, False],
    na_position="last",
).reset_index(drop=True)

summary_path = batch_dir / "all_seeds_best_groups.csv"
result_df.to_csv(summary_path, index=False)
print(f"All-seeds summary saved to: {summary_path}")
print(f"Total seeds evaluated: {len(result_df)}")
print()
print(result_df[["seed", "group_name", "trade_score", "hitrate", "recent20_avg_ret"]].to_string(index=False))
PY

echo ""
echo "Done. Batch results: ${BATCH_DIR}/"
