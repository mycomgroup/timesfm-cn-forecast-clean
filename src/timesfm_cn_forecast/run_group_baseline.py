#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run group-level baseline backtests (ZERO-SHOT with raw model, no adapter)."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd
import random

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from timesfm_cn_forecast.backtest import run_backtest
from timesfm_cn_forecast.run_group_eval import _summarize_best, _filter_by_min_days
from timesfm_cn_forecast.universe import get_stock_universe


def main() -> None:
    parser = argparse.ArgumentParser(description="Zero-shot Baseline runner.")
    parser.add_argument("--group", type=str, required=True, help="Group name, e.g. ind_xxx")
    parser.add_argument("--market-duckdb", type=str, required=True, help="market.duckdb path")
    parser.add_argument("--index-duckdb", type=str, required=True, help="index_market.duckdb path")
    parser.add_argument("--horizon", type=int, default=1, help="Forecast horizon")
    parser.add_argument("--context-lengths", type=str, default="30", help="Eval context lengths (comma-sep)")
    parser.add_argument("--test-days", type=int, default=5, help="Backtest days (rolling days backward)")
    parser.add_argument("--min-days", type=int, default=100, help="Minimum history days")
    parser.add_argument("--start", type=str, default="2021-01-01")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--train-end", type=str, default=None)
    parser.add_argument("--test-start", type=str, default=None)
    parser.add_argument("--test-end", type=str, default=None)
    parser.add_argument("--rolling-windows", type=str, default="20,40,60")
    parser.add_argument("--output-dir", type=str, default="data/baseline_results")
    parser.add_argument("--exclude-file", type=str, default=None, help="Path to txt file of symbols to exclude")
    parser.add_argument("--sample-size", type=int, default=10, help="Number of stocks to sample per group for eval")

    args = parser.parse_args()
    
    context_lengths = [int(x.strip()) for x in args.context_lengths.split(",") if x.strip()]
    output_root = Path(args.output_dir)
    group_dir = output_root / args.group
    group_dir.mkdir(parents=True, exist_ok=True)

    symbols = get_stock_universe(args.group, duckdb_path=args.index_duckdb)
    if not symbols:
        print(f"No symbols found for group {args.group}")
        return

    # Filter out symbols without enough history
    symbols_valid = _filter_by_min_days(symbols, args.market_duckdb, args.min_days)
    if not symbols_valid:
        print(f"No symbols left after min-days for group {args.group}")
        return

    # Filter out excluded symbols
    if args.exclude_file and Path(args.exclude_file).exists():
        with open(args.exclude_file, "r") as f:
            excluded = {line.strip() for line in f if line.strip()}
        symbols_valid = [s for s in symbols_valid if s not in excluded]
        print(f"Excluded {len(excluded)} symbols from consideration. {len(symbols_valid)} remain for {args.group}.")

    if not symbols_valid:
        print(f"No symbols left after exclusion and min-days for group {args.group}")
        return

    # Random sampling to save time across many groups
    random.seed(42)
    eval_symbols = random.sample(symbols_valid, min(args.sample_size, len(symbols_valid)))
    print(f"\n========================================================")
    print(f"Evaluating zero-shot baseline on {len(eval_symbols)} sampled symbols in group: {args.group}")
    print(f"Context lengths: {context_lengths}, Horizon: {args.horizon}, Test Days: {args.test_days}")
    print(f"========================================================")

    results = []
    for symbol in eval_symbols:
        try:
            stats_df = run_backtest(
                symbol=symbol,
                provider="duckdb",
                start_date=args.start,
                end_date=args.end,
                context_lengths=context_lengths,
                horizon=args.horizon,
                test_days=args.test_days,
                adapter_path=None, # ZERO SHOT
                input_csv=None,
                duckdb_path=args.market_duckdb,
                train_end_date=args.train_end,
                test_start_date=args.test_start,
                test_end_date=args.test_end,
                rolling_windows=rolling_windows,
            )
            if stats_df is None or stats_df.empty:
                results.append({"symbol": symbol, "status": "empty"})
                continue

            summary = _summarize_best(stats_df)
            results.append({
                "symbol": symbol,
                **summary,
                "status": "ok",
            })
        except Exception as exc:
            results.append({"symbol": symbol, "status": "error", "error": str(exc)})

    if not results:
        print(f"No valid results produced for group {args.group}.")
        return

    df = pd.DataFrame(results)
    df["horizon"] = args.horizon
    df["zero_shot"] = True
    df["test_days"] = args.test_days
    
    # Calculate group-level summary for quick comparison
    ok_df = df[df["status"] == "ok"]
    avg_hitrate = ok_df["hitrate"].mean() if not ok_df.empty else 0.0
    avg_avg_ret = ok_df["avg_ret"].mean() if not ok_df.empty and "avg_ret" in ok_df.columns else 0.0
    avg_cum_ret = ok_df["cum_ret"].mean() if not ok_df.empty and "cum_ret" in ok_df.columns else 0.0
    avg_profit_factor = ok_df["profit_factor"].replace([float("inf")], pd.NA).dropna().mean() if not ok_df.empty and "profit_factor" in ok_df.columns else 0.0
    avg_recent20 = ok_df["recent20_avg_ret"].mean() if not ok_df.empty and "recent20_avg_ret" in ok_df.columns else 0.0
    
    filename = f"baseline_h{args.horizon}_cl{args.context_lengths}_ss{args.sample_size}.csv"
    output_path = group_dir / filename
    df.to_csv(output_path, index=False)
    
    print(f"\n>>> GROUP [{args.group}] BASELINE RESULTS <<<")
    print(f"Average Hit Rate (last {args.test_days} days): {avg_hitrate:.2f}%")
    print(f"Average AvgRet: {avg_avg_ret:.4f}%")
    print(f"Average CumRet: {avg_cum_ret:.4f}%")
    print(f"Average ProfitFactor: {avg_profit_factor:.4f}")
    print(f"Average Recent20 AvgRet: {avg_recent20:.4f}%")
    print(f"Detailed symbol metrics saved to {output_path}\n")

if __name__ == "__main__":
    main()
    rolling_windows = [int(x.strip()) for x in args.rolling_windows.split(",") if x.strip()]
