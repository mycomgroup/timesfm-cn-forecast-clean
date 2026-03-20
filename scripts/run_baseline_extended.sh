#!/bin/bash
set -euo pipefail

# 扩展版全市场 Baseline (原始模型) 慢慢跑脚本
#
# 这个脚本将：
# 1. 自动拉取我们刚刚新增的 12 个新板块（半导体、AI、软件等）到 DuckDB 数据库。
# 2. 从每个板块随机抽取 20 只没跑过的股票进行 120 天滚动回溯（30天上下文 -> 预测 1天）。
# 3. 生成详细报告

cd "$(dirname "$0")/.."
source scripts/_env.sh
setup_project_env duckdb numpy pandas torch sklearn

# 数据源路径
MARKET_DUCKDB="${MARKET_DUCKDB:-data/market.duckdb}"
INDEX_DUCKDB="${INDEX_DUCKDB:-data/index_market.duckdb}"

HORIZON="${HORIZON:-1}"
CONTEXT_LENGTHS="${CONTEXT_LENGTHS:-30}"
TEST_DAYS="${TEST_DAYS:-120}"
SAMPLE_SIZE="${SAMPLE_SIZE:-20}"
EXCLUDE_FILE="data/tasks/previously_evaluated_symbols.txt"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="data/tasks/baseline_extended_${TIMESTAMP}"
mkdir -p "${OUTPUT_DIR}"

echo "=========================================================================="
echo "🔄 正在刷新 DuckDB 板块数据源..."
# 用脚本内联 Python 运行拉取任务，确保新增的板块全部存在于本地数据库中
"${PYTHON_BIN}" - <<'PY'
import sys
from pathlib import Path
root = Path.cwd()
src = root / "src"
sys.path.insert(0, str(src))

from timesfm_cn_forecast.universe.fetcher import fetch_constituents, INDEX_MAP
from timesfm_cn_forecast.universe.storage import upsert_constituents

duckdb_path = "data/index_market.duckdb"
for group_name in INDEX_MAP:
    try:
        print(f"正在拉取 {group_name} 成份股...")
        df = fetch_constituents(group_name)
        if not df.empty:
            upsert_constituents(df, duckdb_path)
    except Exception as e:
        print(f"跳过 {group_name}: {e}")
print("板块数据刷新完成！")
PY
echo "=========================================================================="


echo "=========================================================================="
echo "🚀 启动大规模精细化 Baseline (原始模型, 无 Patch) 扫描"
echo "上下文长度: ${CONTEXT_LENGTHS} | 预测步长: ${HORIZON} | 回溯天数: ${TEST_DAYS}"
echo "每个板块采样股票数: ${SAMPLE_SIZE}"
echo "输出目录: ${OUTPUT_DIR}"
echo "=========================================================================="

# 读取 DuckDB 里更新过后的所有板块名
GROUP_LIST=$(
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
print(" ".join(df["index_symbol"].tolist()))
PY
)

for group in $GROUP_LIST; do
  echo ">>> 开始评估板块: ${group}"
  "${PYTHON_BIN}" -m timesfm_cn_forecast.run_group_baseline \
    --group "${group}" \
    --market-duckdb "${MARKET_DUCKDB}" \
    --index-duckdb "${INDEX_DUCKDB}" \
    --horizon "${HORIZON}" \
    --context-lengths "${CONTEXT_LENGTHS}" \
    --test-days "${TEST_DAYS}" \
    --sample-size "${SAMPLE_SIZE}" \
    --exclude-file "${EXCLUDE_FILE}" \
    --output-dir "${OUTPUT_DIR}"
done

echo "=========================================================================="
echo "🎉 全市场扩容 Baseline 扫描完成！"
echo "所有各板块的采样结果已保存在: ${OUTPUT_DIR}"
echo "你可以运行专门的合并分析脚本来查看最终的 Hit Rate 排行。"
echo "=========================================================================="
