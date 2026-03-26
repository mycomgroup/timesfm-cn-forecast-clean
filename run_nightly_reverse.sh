#!/bin/bash
PROJECT_ROOT="/Users/yuping/Downloads/git/timesfm-cn-forecast-clean"
cd "$PROJECT_ROOT" || exit 1

# Export environment variables for the evaluation task
export TRAIN_END="2025-12-31" 
export TEST_START="2026-01-01" 
export TEST_END="2026-03-10" 
export SKIP_EXISTING=1 
export ANALYZE=1 
export OUTPUT_DIR="data/tasks/eval_all_groups_resume" 

RUN_TAG=$(date +%Y%m%d_%H%M%S) 
mkdir -p "$PROJECT_ROOT/data/tasks/nightly" 
LOG_FILE="$PROJECT_ROOT/data/tasks/nightly/full_eval_reverse_${RUN_TAG}.log"

# Kill existing process if needed
# pkill -f "run_all_groups_eval_reverse.sh" || true

# Use uv run to ensure the correct environment is used
nohup uv run bash -c "
cd '$PROJECT_ROOT' || exit 1
export PYTHONPATH='$PROJECT_ROOT/src'
export TRAIN_END='$TRAIN_END'
export TEST_START='$TEST_START'
export TEST_END='$TEST_END'
export SKIP_EXISTING='$SKIP_EXISTING'
export ANALYZE='$ANALYZE'
export OUTPUT_DIR='$OUTPUT_DIR'

while true; do 
  bash scripts/run_all_groups_eval_reverse.sh && break 
  echo \"[retry] \$(date '+%F %T') failed, sleep 60s\" 
  sleep 60 
done 
" > "$LOG_FILE" 2>&1 < /dev/null & 

PID=$!
echo "PID=$PID"
echo "LOG_FILE=$LOG_FILE"
