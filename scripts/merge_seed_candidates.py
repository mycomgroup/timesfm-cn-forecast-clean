#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""合并多个 seed_candidates.csv 来源，输出统一的种子候选列表。"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge seed candidate CSVs into one.")
    parser.add_argument(
        "--inputs",
        nargs="+",
        default=[
            "data/tasks/supernode_summary/seed_candidates.csv",
            "data/tasks/supernode_summary_curated_default/seed_candidates.csv",
            "data/tasks/supernode_summary_curated_relaxed/seed_candidates.csv",
        ],
        help="Input seed_candidates CSVs to merge",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/tasks/supernode_summary/seed_candidates_merged.csv",
        help="Output merged CSV path",
    )
    args = parser.parse_args()

    frames = []
    for path_str in args.inputs:
        path = Path(path_str)
        if not path.exists():
            print(f"[WARN] Not found, skipping: {path}")
            continue
        df = pd.read_csv(path, low_memory=False)
        if "symbol" not in df.columns:
            print(f"[WARN] No symbol column in {path}, skipping")
            continue
        df["symbol"] = (
            df["symbol"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.zfill(6)
        )
        frames.append(df)

    if not frames:
        raise RuntimeError("No valid seed_candidates CSVs found")

    merged = pd.concat(frames, ignore_index=True)

    # Deduplicate: keep the row with highest avg_trade_score per symbol
    score_col = "avg_trade_score"
    if score_col in merged.columns:
        merged[score_col] = pd.to_numeric(merged[score_col], errors="coerce")
        merged = merged.sort_values(score_col, ascending=False, na_position="last")
    merged = merged.drop_duplicates(subset="symbol", keep="first").reset_index(drop=True)

    # Re-rank
    merged["seed_rank"] = range(1, len(merged) + 1)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    print(f"Merged {len(merged)} unique seeds -> {output_path}")
    print("Seeds:", merged["symbol"].tolist())


if __name__ == "__main__":
    main()
