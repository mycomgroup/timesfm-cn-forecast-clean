#!/bin/bash
# A/B Test Script: Individual vs Group Training
# Usage: bash scripts/run_ab_test.sh

export PYTHONPATH=$PYTHONPATH:$(pwd)/src
mkdir -p data/tasks/ab_test/

TARGETS=$(python3 -c "import json; print(' '.join(json.load(open('data/ab_test_targets.json'))))")

echo "Starting A/B Test for: $TARGETS"

for symbol in $TARGETS; do
    echo "--------------------------------------------------"
    echo "Processing $symbol..."
    
    # 1. Run Individual Training (Sample size = 1)
    echo "Running Individual Training for $symbol..."
    /opt/anaconda3/bin/python -u -m timesfm_cn_forecast.run_group_eval \
        --group "single_$symbol" \
        --market-duckdb data/market.duckdb \
        --index-duckdb data/index_market.duckdb \
        --train-days 300 \
        --horizon 1 \
        --context-len 30 \
        --test-days 120 \
        --output-dir data/tasks/ab_test/individual/ \
        | tee "data/tasks/ab_test/individual_${symbol}.log"

    # Note: Resonance group training results are already being generated in the main resonance experiment.
    # We will compare them once both are ready.
done

echo "A/B Test Training Phase Complete."
