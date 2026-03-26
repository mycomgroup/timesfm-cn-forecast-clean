#!/bin/bash
cd /Users/yuping/Downloads/git/timesfm-cn-forecast-clean
export TRAIN_END="2025-12-31" 
export TEST_START="2026-01-01" 
export TEST_END="2026-03-10" 
export SKIP_EXISTING=1 
export ANALYZE=1 
export OUTPUT_DIR="data/tasks/eval_all_groups_resume" 
RUN_TAG=$(date +%Y%m%d_%H%M%S) 
mkdir -p data/tasks/nightly 
nohup bash -lc ' 
cd /Users/yuping/Downloads/git/timesfm-cn-forecast-clean 
while true; do 
uv run bash scripts/run_all_groups_eval.sh && break 
echo "[retry] $(date "+%F %T") failed, sleep 60s" 
sleep 60 
done 
' > "data/tasks/nightly/full_eval_${RUN_TAG}.log" 2>&1 < /dev/null & echo "PID=$!"