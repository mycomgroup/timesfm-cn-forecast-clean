#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
export PYTHONPATH=src

INPUT_CSV="${1:-data/tasks/daily_top_candidates.csv}"
OUTPUT_CSV="${2:-data/tasks/daily_weights.csv}"
TOP_K="${TOP_K:-20}"

INPUT_CSV="${INPUT_CSV}" OUTPUT_CSV="${OUTPUT_CSV}" TOP_K="${TOP_K}" python - <<'PY'
import os
import pandas as pd
from timesfm_cn_forecast.daily_weights import build_daily_weights

input_csv = os.environ.get("INPUT_CSV", "data/tasks/daily_top_candidates.csv")
output_csv = os.environ.get("OUTPUT_CSV", "data/tasks/daily_weights.csv")
top_k = int(os.environ.get("TOP_K", "20"))

df = pd.read_csv(input_csv)
out = build_daily_weights(df, top_k=top_k)
out.to_csv(output_csv, index=False)
print(f"saved: {output_csv}")
PY
