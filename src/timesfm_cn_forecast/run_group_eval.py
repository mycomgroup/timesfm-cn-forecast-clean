#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run group-level adapter training and per-stock backtests."""

from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from timesfm_cn_forecast.backtest import run_backtest
from timesfm_cn_forecast.features import FeatureExtractor, get_feature_names
from timesfm_cn_forecast.finetuning import save_adapter, train_linear_adapter
from timesfm_cn_forecast.modeling import 默认模型目录, 加载模型
from timesfm_cn_forecast.providers import DataRequest, load_historical_data, normalize_symbol
from timesfm_cn_forecast.universe import get_stock_universe


DEFAULT_CONTEXT_LENGTHS = [30, 60, 90, 128, 256, 512]


@dataclass
class TrainSampleStore:
    features: List[np.ndarray]
    targets: List[float]
    base_preds: List[float]


def _parse_context_lengths(raw: str | None) -> List[int]:
    if not raw:
        return DEFAULT_CONTEXT_LENGTHS
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return [int(item) for item in items]


def _parse_rolling_windows(raw: str | None) -> List[int]:
    if not raw:
        return [20, 40, 60]
    return [int(item.strip()) for item in raw.split(",") if item.strip()]


def _chunked(items: Iterable[str], size: int) -> Iterable[List[str]]:
    batch: List[str] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _filter_by_min_days(symbols: List[str], duckdb_path: str, min_days: int) -> List[str]:
    if min_days <= 0 or not symbols:
        return symbols

    try:
        import duckdb
    except ImportError as exc:
        raise ImportError("duckdb is required for reading market.duckdb") from exc

    raw_to_db = {sym: normalize_symbol(sym, "db") for sym in symbols}
    db_symbols = sorted(set(raw_to_db.values()))
    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        valid_db: set[str] = set()
        for chunk in _chunked(db_symbols, 500):
            df = con.execute(
                """
                SELECT symbol, COUNT(*) AS n
                FROM daily_data
                WHERE symbol IN (SELECT * FROM UNNEST(?))
                GROUP BY symbol
                """,
                [chunk],
            ).fetchdf()
            if not df.empty:
                valid_db.update(df.loc[df["n"] >= min_days, "symbol"].tolist())
    finally:
        con.close()

    return [sym for sym, db_sym in raw_to_db.items() if db_sym in valid_db]


def _build_training_samples(
    symbols: List[str],
    args: argparse.Namespace,
    feature_names: List[str],
    model=None,
) -> TrainSampleStore:
    store = TrainSampleStore(features=[], targets=[], base_preds=[])
    all_symbol_data = []

    for symbol in symbols:
        req_end = args.train_end if args.train_end else args.end
        req = DataRequest(
            provider="duckdb",
            symbol=symbol,
            start=args.start,
            end=req_end,
            kline=True,
            value_column="close",
            duckdb_path=args.market_duckdb,
        )
        df = load_historical_data(req)
        if df.empty:
            continue

        if args.train_end:
            df = df[df.index <= pd.Timestamp(args.train_end)]

        df = df.ffill().bfill()
        if args.train_days:
            df = df.tail(args.train_days + args.context_len + args.horizon)

        prices = df["value"].to_numpy(dtype=np.float32)
        ohlcv_cols = ["open", "high", "low", "close", "volume"]
        ohlcv = df[ohlcv_cols].to_numpy(dtype=np.float32) if all(c in df.columns for c in ohlcv_cols) else None

        n_samples = len(prices) - args.context_len - args.horizon + 1
        if n_samples <= 0:
            continue

        contexts = []
        targets = []
        ohlcv_contexts = []
        for i in range(n_samples):
            contexts.append(prices[i : i + args.context_len])
            targets.append(float(prices[i + args.context_len + args.horizon - 1]))
            ohlcv_contexts.append(ohlcv[i : i + args.context_len] if ohlcv is not None else None)

        all_symbol_data.append(
            {
                "symbol": symbol,
                "contexts": contexts,
                "targets": targets,
                "ohlcv_contexts": ohlcv_contexts,
            }
        )

    if not all_symbol_data:
        return store

    if model:
        all_contexts_flat = []
        for data in all_symbol_data:
            all_contexts_flat.extend([c.astype(np.float32) for c in data["contexts"]])

        chunk_size = 128
        all_base_preds_flat = []
        for i in range(0, len(all_contexts_flat), chunk_size):
            chunk = all_contexts_flat[i : i + chunk_size]
            pts, _ = model.forecast(horizon=1, inputs=chunk)
            all_base_preds_flat.extend(pts[:, 0].tolist())

        curr = 0
        for data in all_symbol_data:
            count = len(data["contexts"])
            data["base_preds"] = all_base_preds_flat[curr : curr + count]
            curr += count
    else:
        for data in all_symbol_data:
            data["base_preds"] = [float(c[-1]) for c in data["contexts"]]

    for data in all_symbol_data:
        for i in range(len(data["contexts"])):
            base_pred = data["base_preds"][i]
            feats = FeatureExtractor.compute(
                data["contexts"][i],
                base_pred,
                ohlcv_context=data["ohlcv_contexts"][i],
                feature_names=feature_names,
            )
            store.features.append(feats)
            store.targets.append(data["targets"][i])
            store.base_preds.append(base_pred)

    return store


def _train_group_adapter(symbols: List[str], args: argparse.Namespace, output_dir: Path, model=None) -> Path:
    feature_names = get_feature_names(args.feature_set)
    samples = _build_training_samples(symbols, args, feature_names, model=model)

    if not samples.features:
        raise RuntimeError("训练样本为空，无法训练分组适配器。")

    train_x = np.array(samples.features, dtype=np.float32)
    train_y = np.array(samples.targets, dtype=np.float32)
    train_base = np.array(samples.base_preds, dtype=np.float32)
    train_x = np.nan_to_num(train_x, nan=0.0, posinf=0.0, neginf=0.0)

    weights = train_linear_adapter(
        train_X=train_x,
        train_y=train_y,
        train_base=train_base,
        context_len=args.context_len,
        horizon_len=args.horizon,
        feature_names=feature_names,
        stock_code=f"group:{args.group}",
        model_type=args.model_type,
        ridge_alpha=args.ridge_alpha,
        huber_epsilon=args.huber_epsilon,
    )

    adapter_path = output_dir / "adapter.pth"
    save_adapter(weights, str(adapter_path))
    return adapter_path


def _summarize_best(stats_df: pd.DataFrame) -> dict[str, float]:
    best_idx = stats_df["RMSE"].idxmin()
    row = stats_df.loc[best_idx]
    summary = {
        "best_context_len": int(row["ContextLen"]),
        "rmse": float(row["RMSE"]),
        "hitrate": float(row["HitRate"]),
        "mae": float(row["MAE"]),
        "mape": float(row["MAPE"]),
        "avg_ret": float(row["AvgRet"]),
        "cum_ret": float(row["CumRet"]),
        "avg_trade_ret": float(row["AvgTradeRet"]),
        "avg_win": float(row["AvgWin"]),
        "avg_loss": float(row["AvgLoss"]),
        "profit_factor": float(row["ProfitFactor"]),
        "win_loss_ratio": float(row["WinLossRatio"]),
        "max_drawdown": float(row["MaxDrawdown"]),
        "sharpe": float(row["Sharpe"]),
        "sortino": float(row["Sortino"]),
        "calmar": float(row["Calmar"]),
        "volatility": float(row["Volatility"]),
        "num_trades": float(row["NumTrades"]),
        "exposure": float(row["Exposure"]),
    }
    for window in (20, 40, 60):
        summary[f"recent{window}_avg_ret"] = float(row.get(f"Recent{window}AvgRet", 0.0))
        summary[f"recent{window}_cum_ret"] = float(row.get(f"Recent{window}CumRet", 0.0))
        summary[f"recent{window}_hitrate"] = float(row.get(f"Recent{window}HitRate", 0.0))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Group-level training + evaluation runner.")
    parser.add_argument("--group", type=str, required=True, help="Group name, e.g. ind_xxx")
    parser.add_argument("--market-duckdb", type=str, required=True, help="market.duckdb path")
    parser.add_argument("--index-duckdb", type=str, required=True, help="index_market.duckdb path")
    parser.add_argument("--feature-set", type=str, default="full")
    parser.add_argument("--train-days", type=int, default=60)
    parser.add_argument("--horizon", type=int, default=1)
    parser.add_argument("--context-len", type=int, default=60)
    parser.add_argument("--context-lengths", type=str, default=None)
    parser.add_argument("--test-days", type=int, default=20)
    parser.add_argument("--min-days", type=int, default=1000)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--train-end", type=str, default=None)
    parser.add_argument("--test-start", type=str, default=None)
    parser.add_argument("--test-end", type=str, default=None)
    parser.add_argument("--rolling-windows", type=str, default="20,40,60")
    parser.add_argument("--output-dir", type=str, default="data/research")
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--model-type", type=str, default="ridge", choices=["lstsq", "ridge", "huber"])
    parser.add_argument("--ridge-alpha", type=float, default=0.1)
    parser.add_argument("--huber-epsilon", type=float, default=1.35)

    args = parser.parse_args()
    context_lengths = _parse_context_lengths(args.context_lengths)
    rolling_windows = _parse_rolling_windows(args.rolling_windows)

    output_root = Path(args.output_dir)
    group_dir = output_root / args.group
    group_dir.mkdir(parents=True, exist_ok=True)

    symbols = get_stock_universe(args.group, duckdb_path=args.index_duckdb)
    if not symbols:
        raise RuntimeError(f"No symbols found for group: {args.group}")

    symbols_for_train = _filter_by_min_days(symbols, args.market_duckdb, args.min_days)
    if not symbols_for_train:
        raise RuntimeError("No symbols left for training after min-days filter.")

    model_dir = 默认模型目录()
    base_model = 加载模型(model_dir)
    adapter_path = _train_group_adapter(symbols_for_train, args, group_dir, model=base_model)

    symbols_for_eval = symbols
    if args.sample_size and args.sample_size < len(symbols):
        random.seed(42)
        symbols_for_eval = random.sample(symbols, args.sample_size)

    results = []
    for symbol in symbols_for_eval:
        try:
            stats_df = run_backtest(
                symbol=symbol,
                provider="duckdb",
                start_date=args.start,
                end_date=args.end,
                context_lengths=context_lengths,
                horizon=args.horizon,
                test_days=args.test_days,
                adapter_path=str(adapter_path),
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
            results.append({"symbol": symbol, **summary, "status": "ok"})
        except Exception as exc:
            results.append({"symbol": symbol, "status": "error", "error": str(exc)})

    df = pd.DataFrame(results)
    df["feature_set"] = args.feature_set
    df["train_days"] = args.train_days
    df["horizon"] = args.horizon
    df["context_len"] = args.context_len
    df["sample_size"] = args.sample_size
    df["train_end"] = args.train_end
    df["test_start"] = args.test_start
    df["test_end"] = args.test_end

    filename = (
        f"results_{args.feature_set}_td{args.train_days}_h{args.horizon}_"
        f"cl{args.context_len}_ss{args.sample_size if args.sample_size else 'all'}.csv"
    )
    output_path = group_dir / filename
    df.to_csv(output_path, index=False)

    meta = {
        "group": args.group,
        "train_end": args.train_end,
        "test_start": args.test_start,
        "test_end": args.test_end,
        "rolling_windows": rolling_windows,
        "use_patch": True,
        "mode": "group",
        "model_type": args.model_type,
        "horizon": args.horizon,
        "feature_set": args.feature_set,
    }
    with open(group_dir / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"Group investigation results saved to {output_path}")


if __name__ == "__main__":
    main()
