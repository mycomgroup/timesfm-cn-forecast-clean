#!/bin/bash
set -euo pipefail

# Usage:
#   bash scripts/run_seed_group_eval.sh <seed_symbol> [seed_definition_json]
#
# Example:
#   bash scripts/run_seed_group_eval.sh 600519

cd "$(dirname "$0")/.."
source "$(dirname "$0")/_env.sh"
setup_project_env duckdb numpy pandas torch sklearn

SEED_RAW="${1:-}"
if [ -z "${SEED_RAW}" ]; then
  echo "Usage: $0 <seed_symbol> [seed_definition_json]"
  exit 1
fi
SEED=$(echo "${SEED_RAW}" | tr -cd '0-9')
if [ -z "${SEED}" ]; then
  echo "Invalid seed symbol: ${SEED_RAW}"
  exit 1
fi
SEED=$(printf "%06s" "${SEED}" | tr ' ' '0')

GROUP_DEF="${2:-data/group_definitions/seed_${SEED}.json}"
if [ ! -f "${GROUP_DEF}" ]; then
  echo "Seed group definition not found: ${GROUP_DEF}"
  exit 1
fi
GROUP_DEF_DIR="$(cd "$(dirname "${GROUP_DEF}")" && pwd)"

MARKET_DUCKDB="${MARKET_DUCKDB:-data/market.duckdb}"
INDEX_DUCKDB="${INDEX_DUCKDB:-data/index_market.duckdb}"
FEATURE_SET="${FEATURE_SET:-full}"
TRAIN_DAYS="${TRAIN_DAYS:-200}"
HORIZON="${HORIZON:-1}"
CONTEXT_LEN="${CONTEXT_LEN:-30}"
CONTEXT_LENGTHS="${CONTEXT_LENGTHS:-30,60}"
TEST_DAYS="${TEST_DAYS:-60}"
MIN_DAYS="${MIN_DAYS:-300}"
SAMPLE_SIZE="${SAMPLE_SIZE:-}"
TRAIN_END="${TRAIN_END:-2025-12-31}"
TEST_START="${TEST_START:-2026-01-01}"
TEST_END="${TEST_END:-2026-03-10}"
ROLLING_WINDOWS="${ROLLING_WINDOWS:-20,40,60}"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TASK_DIR="data/tasks/seed_group_eval_${SEED}_${TIMESTAMP}"
OUTPUT_DIR="${TASK_DIR}/groups"
mkdir -p "${OUTPUT_DIR}"

EXTRA_ARGS=()
if [ -n "${SAMPLE_SIZE}" ]; then
  EXTRA_ARGS+=(--sample-size "${SAMPLE_SIZE}")
fi

GROUP_LIST=$(
  "${PYTHON_BIN}" - "${GROUP_DEF}" "${SEED}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
seed = str(sys.argv[2]).zfill(6)
payload = json.loads(path.read_text(encoding="utf-8"))

groups = []
if isinstance(payload, dict) and isinstance(payload.get("groups"), list):
    for item in payload["groups"]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        syms = item.get("symbols", [])
        if not name or not isinstance(syms, list):
            continue
        norm_syms = [str(s).replace(".0", "").zfill(6) for s in syms]
        if seed in norm_syms:
            groups.append(name)

print(" ".join(groups))
PY
)

if [ -z "${GROUP_LIST}" ]; then
  echo "No seed groups found in ${GROUP_DEF} for seed=${SEED}"
  exit 1
fi

read -r -a GROUP_ARR <<< "${GROUP_LIST}"

echo ">>> Register dynamic seed groups into index DuckDB"
"${PYTHON_BIN}" -m timesfm_cn_forecast.universe \
  --index "${GROUP_ARR[@]}" \
  --duckdb-path "${INDEX_DUCKDB}" \
  --group-definitions-dir "${GROUP_DEF_DIR}"

for group in "${GROUP_ARR[@]}"; do
  echo ">>> Evaluating seed group variant: ${group}"
  "${PYTHON_BIN}" -m timesfm_cn_forecast.run_group_eval \
    --group "${group}" \
    --market-duckdb "${MARKET_DUCKDB}" \
    --index-duckdb "${INDEX_DUCKDB}" \
    --feature-set "${FEATURE_SET}" \
    --train-days "${TRAIN_DAYS}" \
    --horizon "${HORIZON}" \
    --context-len "${CONTEXT_LEN}" \
    --context-lengths "${CONTEXT_LENGTHS}" \
    --test-days "${TEST_DAYS}" \
    --min-days "${MIN_DAYS}" \
    --train-end "${TRAIN_END}" \
    --test-start "${TEST_START}" \
    --test-end "${TEST_END}" \
    --rolling-windows "${ROLLING_WINDOWS}" \
    --must-include-symbol "${SEED}" \
    --output-dir "${OUTPUT_DIR}" \
    ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}
done

SUMMARY_DIR="${TASK_DIR}/summary"
mkdir -p "${SUMMARY_DIR}"

"${PYTHON_BIN}" - "${OUTPUT_DIR}" "${SEED}" "${SUMMARY_DIR}" <<'PY'
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

output_root = Path(sys.argv[1])
seed = str(sys.argv[2]).zfill(6)
summary_dir = Path(sys.argv[3])

rows = []
for group_dir in sorted(output_root.iterdir()):
    if not group_dir.is_dir():
        continue

    full_path = group_dir / "group_full_results.csv"
    if full_path.exists():
        df = pd.read_csv(full_path, low_memory=False)
    else:
        candidates = sorted(group_dir.glob("results_*.csv"))
        if not candidates:
            continue
        df = pd.read_csv(candidates[-1], low_memory=False)

    if "symbol" not in df.columns:
        continue
    df["symbol"] = df["symbol"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    if "status" in df.columns:
        df = df[df["status"] == "ok"].copy()
    if df.empty:
        continue

    seed_row = df[df["symbol"] == seed]
    if seed_row.empty:
        continue
    row = seed_row.iloc[0]

    rows.append(
        {
            "group_name": group_dir.name,
            "seed": seed,
            "trade_score": float(pd.to_numeric(row.get("trade_score"), errors="coerce")),
            "hitrate": float(pd.to_numeric(row.get("hitrate"), errors="coerce")),
            "avg_ret": float(pd.to_numeric(row.get("avg_ret"), errors="coerce")),
            "cum_ret": float(pd.to_numeric(row.get("cum_ret"), errors="coerce")),
            "profit_factor": float(pd.to_numeric(row.get("profit_factor"), errors="coerce")),
            "max_drawdown": float(pd.to_numeric(row.get("max_drawdown"), errors="coerce")),
            "recent20_avg_ret": float(pd.to_numeric(row.get("recent20_avg_ret"), errors="coerce")),
            "recent60_avg_ret": float(pd.to_numeric(row.get("recent60_avg_ret"), errors="coerce")),
        }
    )

if not rows:
    raise RuntimeError(f"Seed {seed} was not found in evaluated group results")

summary_df = pd.DataFrame(rows)
summary_df = summary_df.sort_values(
    by=["trade_score", "recent20_avg_ret", "hitrate", "profit_factor"],
    ascending=[False, False, False, False],
    na_position="last",
).reset_index(drop=True)
summary_df["seed_group_rank"] = np.arange(1, len(summary_df) + 1)
summary_path = summary_dir / "seed_group_compare.csv"
summary_df.to_csv(summary_path, index=False)

best = summary_df.iloc[0].to_dict()
best_path = summary_dir / "best_seed_group.json"
best_path.write_text(json.dumps(best, ensure_ascii=False, indent=2), encoding="utf-8")

print(f"Seed comparison saved to: {summary_path}")
print(f"Best seed group saved to: {best_path}")
print(f"BEST => {best['group_name']} | trade_score={best['trade_score']:.4f} | recent20={best['recent20_avg_ret']:.4f}")
PY

echo "Done. Seed evaluation task: ${TASK_DIR}"
