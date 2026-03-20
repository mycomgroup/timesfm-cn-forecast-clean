#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}/.."
source scripts/_env.sh
setup_project_env duckdb

INDEX_DB="${PROJECT_ROOT}/data/index_market.duckdb"

LOG_FILE="/tmp/census_v3_results.log"
echo ">>> Census V3 Start at $(date)" > "${LOG_FILE}"

# 使用 Python 脚本生成清洁列表到数组
mapfile -t GROUPS < <("${PYTHON_BIN}" - "${INDEX_DB}" <<'PY'
import sys
import duckdb
try:
    conn = duckdb.connect(sys.argv[1])
    res = conn.execute("SELECT DISTINCT index_symbol FROM index_constituents").fetchall()
    symbols = [r[0] for r in res]
    allowed = ("ind_", "con_", "resonance_", "vol_", "HS300", "ZZ500", "ZZ800", "CYBZ", "ZXBZ", "small", "A", "AA")
    valid = sorted([s for s in symbols if s.startswith(allowed)])
    print("\n".join(valid))
except Exception as e:
    print(f"ERROR_IN_PY: {e}", file=sys.stderr)
PY
)

COUNT="${#GROUPS[@]}"
echo ">>> Found ${COUNT} groups in memory" >> "${LOG_FILE}"
if [ "${COUNT}" -eq 0 ]; then
    echo "ERROR: no groups found, aborting." | tee -a "${LOG_FILE}" >&2
    exit 1
fi

# 循环执行
i=0
for line in "${GROUPS[@]}"; do
    [ -z "$line" ] && continue
    i=$((i+1))

    echo ">>> [${i}/${COUNT}] Processing Group: ${line}" >> "${LOG_FILE}"

    # 调用执行器
    bash scripts/run_one_group_eval.sh \
        "${line}" \
        "full" 200 1 30 40 300 \
        >> "${LOG_FILE}" 2>&1 || echo "!! FAILED: ${line}" >> "${LOG_FILE}"

    # 对于普查，我们暂时每跑一个也打印一下进度到 stderr (让 user 看到)
    echo "Processed ${i}/${COUNT}: ${line}" >&2
done

echo ">>> CENSUS V3 COMPLETE <<<" >> "${LOG_FILE}"
