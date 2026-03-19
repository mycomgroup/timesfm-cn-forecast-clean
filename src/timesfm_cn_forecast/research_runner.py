#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Run A/B research in batch")
    parser.add_argument("--groups-file", type=str, required=True, help="CSV with column group")
    parser.add_argument("--market-duckdb", type=str, required=True)
    parser.add_argument("--index-duckdb", type=str, required=True)
    parser.add_argument("--output-dir", type=str, default="data/tasks/research_runner")
    parser.add_argument("--train-end", type=str, default="2025-12-31")
    parser.add_argument("--test-start", type=str, default="2026-01-01")
    parser.add_argument("--test-end", type=str, default="2026-03-10")
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--model-type", type=str, default="ridge", choices=["lstsq", "ridge", "huber"])
    args = parser.parse_args()

    groups = pd.read_csv(args.groups_file)["group"].astype(str).tolist()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for group in groups:
        cmd = [
            sys.executable, "-m", "timesfm_cn_forecast.run_group_eval",
            "--group", group,
            "--market-duckdb", args.market_duckdb,
            "--index-duckdb", args.index_duckdb,
            "--train-end", args.train_end,
            "--test-start", args.test_start,
            "--test-end", args.test_end,
            "--sample-size", str(args.sample_size),
            "--model-type", args.model_type,
            "--output-dir", str(out_dir),
        ]
        ret = subprocess.run(cmd, capture_output=True, text=True)
        rows.append({"group": group, "status": "ok" if ret.returncode == 0 else "error", "message": ret.stderr[-2000:]})

    pd.DataFrame(rows).to_csv(out_dir / "ab_summary.csv", index=False)
    print(f"Saved: {out_dir / 'ab_summary.csv'}")


if __name__ == "__main__":
    main()
