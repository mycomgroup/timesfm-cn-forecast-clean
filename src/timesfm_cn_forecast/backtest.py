#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""回测验证与参数优化脚本。"""

from __future__ import annotations

import argparse
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .modeling import 默认模型目录, load_advanced_model, 加载模型, 运行预测
from .providers import DataRequest, load_historical_data


def _to_timestamp(date_str: Optional[str]) -> Optional[pd.Timestamp]:
    if not date_str:
        return None
    return pd.Timestamp(date_str)


def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"])
        out = out.sort_values("date").set_index("date")
    else:
        out.index = pd.to_datetime(out.index)
        out = out.sort_index()
    return out


def calculate_trading_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
) -> Dict[str, float]:
    """计算交易层指标。"""
    if len(y_true) <= 1:
        return {
            "HitRate": 0.0,
            "AvgRet": 0.0,
            "CumRet": 0.0,
            "AvgTradeRet": 0.0,
            "AvgWin": 0.0,
            "AvgLoss": 0.0,
            "ProfitFactor": 0.0,
            "WinLossRatio": 0.0,
            "MaxDrawdown": 0.0,
            "Sharpe": 0.0,
            "Sortino": 0.0,
            "Calmar": 0.0,
            "Volatility": 0.0,
            "NumTrades": 0.0,
            "Exposure": 0.0,
        }

    eps = 1e-8
    diff_true = y_true[1:] - y_true[:-1]
    diff_pred = y_pred[1:] - y_true[:-1]

    true_dir = np.where(np.abs(diff_true) > eps, np.sign(diff_true), 0)
    pred_dir = np.where(np.abs(diff_pred) > eps, np.sign(diff_pred), 0)
    hit_rate = float(np.mean(true_dir == pred_dir) * 100) if len(true_dir) > 0 else 0.0

    returns = np.divide(diff_true, y_true[:-1], out=np.zeros_like(diff_true), where=np.abs(y_true[:-1]) > eps)
    signals = (diff_pred > eps).astype(float)
    strategy_returns = signals * returns

    avg_return = float(np.mean(strategy_returns) * 100) if len(strategy_returns) > 0 else 0.0
    cum_return = float((np.prod(1 + strategy_returns) - 1) * 100) if len(strategy_returns) > 0 else 0.0

    trade_returns = strategy_returns[signals > 0]
    num_trades = int(len(trade_returns))
    exposure = float(np.mean(signals) * 100) if len(signals) > 0 else 0.0

    if num_trades > 0:
        avg_trade_return = float(np.mean(trade_returns) * 100)
        positive_returns = trade_returns[trade_returns > 0]
        negative_returns = trade_returns[trade_returns < 0]
        avg_win = float(np.mean(positive_returns) * 100) if len(positive_returns) > 0 else 0.0
        avg_loss = float(np.mean(negative_returns) * 100) if len(negative_returns) > 0 else 0.0
    else:
        avg_trade_return = 0.0
        positive_returns = np.array([], dtype=float)
        negative_returns = np.array([], dtype=float)
        avg_win = 0.0
        avg_loss = 0.0

    gross_profit = float(np.sum(positive_returns))
    gross_loss = float(-np.sum(negative_returns))
    if gross_loss > eps:
        profit_factor = float(gross_profit / gross_loss)
    elif gross_profit > eps:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0

    if len(negative_returns) > 0 and abs(avg_loss) > eps:
        win_loss_ratio = float(abs(avg_win / avg_loss))
    elif len(positive_returns) > 0:
        win_loss_ratio = float("inf")
    else:
        win_loss_ratio = 0.0

    equity_curve = np.cumprod(1 + strategy_returns) if len(strategy_returns) > 0 else np.array([], dtype=float)
    if len(equity_curve) > 0:
        running_peak = np.maximum.accumulate(equity_curve)
        drawdowns = equity_curve / running_peak - 1.0
        max_drawdown = float(np.min(drawdowns) * 100)
    else:
        max_drawdown = 0.0

    ret_std = float(np.std(strategy_returns, ddof=0)) if len(strategy_returns) > 0 else 0.0
    sharpe = float(np.mean(strategy_returns) / ret_std) if ret_std > eps else 0.0
    downside = strategy_returns[strategy_returns < 0]
    downside_std = float(np.std(downside, ddof=0)) if len(downside) > 0 else 0.0
    sortino = float(np.mean(strategy_returns) / downside_std) if downside_std > eps else 0.0

    if abs(max_drawdown) > eps:
        calmar = float(cum_return / abs(max_drawdown))
    elif cum_return > eps:
        calmar = float("inf")
    else:
        calmar = 0.0

    return {
        "HitRate": hit_rate,
        "AvgRet": avg_return,
        "CumRet": cum_return,
        "AvgTradeRet": avg_trade_return,
        "AvgWin": avg_win,
        "AvgLoss": avg_loss,
        "ProfitFactor": profit_factor,
        "WinLossRatio": win_loss_ratio,
        "MaxDrawdown": max_drawdown,
        "Sharpe": sharpe,
        "Sortino": sortino,
        "Calmar": calmar,
        "Volatility": float(ret_std * 100),
        "NumTrades": float(num_trades),
        "Exposure": exposure,
    }


def summarize_recent_windows(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    rolling_windows: List[int],
) -> Dict[str, float]:
    """统计最近窗口交易指标。"""
    eps = 1e-8
    out: Dict[str, float] = {}

    if len(y_true) <= 1:
        for window in rolling_windows:
            out[f"Recent{window}AvgRet"] = 0.0
            out[f"Recent{window}CumRet"] = 0.0
            out[f"Recent{window}HitRate"] = 0.0
        return out

    diff_true = y_true[1:] - y_true[:-1]
    diff_pred = y_pred[1:] - y_true[:-1]
    returns = np.divide(diff_true, y_true[:-1], out=np.zeros_like(diff_true), where=np.abs(y_true[:-1]) > eps)
    signals = (diff_pred > eps).astype(float)
    strategy_returns = signals * returns
    hits = (np.sign(diff_true) == np.sign(diff_pred)).astype(float)

    for window in rolling_windows:
        w = max(int(window), 1)
        recent_rets = strategy_returns[-w:]
        recent_hits = hits[-w:]
        out[f"Recent{w}AvgRet"] = float(np.mean(recent_rets) * 100) if len(recent_rets) else 0.0
        out[f"Recent{w}CumRet"] = float((np.prod(1 + recent_rets) - 1) * 100) if len(recent_rets) else 0.0
        out[f"Recent{w}HitRate"] = float(np.mean(recent_hits) * 100) if len(recent_hits) else 0.0
    return out


def calculate_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    rolling_windows: List[int],
) -> Dict[str, float]:
    """计算预测层 + 交易层综合指标。"""
    if len(y_true) == 0:
        return {}

    mae = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
    ape = np.abs(y_true - y_pred)
    mape = float(np.mean(np.divide(ape, y_true, out=np.zeros_like(ape), where=y_true != 0)) * 100)

    result = {
        "MAE": mae,
        "RMSE": rmse,
        "MAPE": mape,
    }
    result.update(calculate_trading_metrics(y_true, y_pred))
    result.update(summarize_recent_windows(y_true, y_pred, rolling_windows))
    return result


def run_backtest(
    symbol: str,
    provider: str,
    start_date: str,
    end_date: Optional[str],
    context_lengths: List[int],
    horizon: int = 5,
    test_days: int = 20,
    adapter_path: Optional[str] = None,
    input_csv: Optional[str] = None,
    duckdb_path: Optional[str] = None,
    train_end_date: Optional[str] = None,
    test_start_date: Optional[str] = None,
    test_end_date: Optional[str] = None,
    rolling_windows: Optional[List[int]] = None,
):
    """运行滚动回测。"""
    print(f"开始为 {symbol} 运行回测验证 (Provider: {provider})...")

    rolling_windows = rolling_windows or [20, 40, 60]
    max_context = max(context_lengths)

    req = DataRequest(
        symbol=symbol,
        provider=provider,
        start=start_date,
        end=end_date,
        kline=True if adapter_path else False,
        input_csv=input_csv,
        value_column="value" if provider == "local" else "close",
        duckdb_path=duckdb_path,
    )
    df = load_historical_data(req)
    if df.empty:
        return None

    df = _ensure_datetime_index(df).ffill().bfill()

    ts_train_end = _to_timestamp(train_end_date)
    ts_test_start = _to_timestamp(test_start_date)
    ts_test_end = _to_timestamp(test_end_date)

    if ts_train_end is not None and ts_test_start is None:
        ts_test_start = ts_train_end + pd.Timedelta(days=1)
    if ts_test_start is None:
        ts_test_start = pd.Timestamp(df.index[-1]) - pd.Timedelta(days=max(int(test_days), 1) * 3)

    if ts_test_end is None:
        ts_test_end = pd.Timestamp(df.index[-1])

    df_eval = df[(df.index >= ts_test_start) & (df.index <= ts_test_end)]
    if df_eval.empty:
        print(f"测试窗口无数据: [{ts_test_start.date()}, {ts_test_end.date()}]")
        return None

    prices = df["value"].to_numpy(dtype=np.float32)
    dates = pd.to_datetime(df.index)

    model_dir = 默认模型目录()
    if adapter_path:
        model = load_advanced_model(model_dir, adapter_path)
        ohlcv_cols = ["open", "high", "low", "close", "volume"]
        ohlcv_data = df[ohlcv_cols].to_numpy(dtype=np.float32) if all(c in df.columns for c in ohlcv_cols) else None
    else:
        model = 加载模型(model_dir)
        ohlcv_data = None

    all_stats = []

    for clen in context_lengths:
        errors = []
        preds = []
        actuals = []

        for i in range(clen, len(prices) - horizon + 1):
            pred_date = dates[i]
            if pred_date < ts_test_start or pred_date > ts_test_end:
                continue
            if ts_train_end is not None and pred_date <= ts_train_end:
                continue

            context = prices[i - clen : i]
            target = float(prices[i + horizon - 1])

            if adapter_path:
                ohlcv_context = [ohlcv_data[i - clen : i]] if ohlcv_data is not None else None
                pts, _ = model.forecast(inputs=[context.astype(np.float32)], horizon=horizon, ohlcv_inputs=ohlcv_context)
                pred_val = float(pts[0, horizon - 1])
            else:
                pts, _ = 运行预测(model, context.astype(np.float32), clen, horizon)
                pred_val = float(pts[horizon - 1])

            preds.append(pred_val)
            actuals.append(target)
            errors.append(abs(pred_val - target))

        if len(actuals) < 2:
            continue

        metrics = calculate_metrics(np.asarray(actuals, dtype=np.float32), np.asarray(preds, dtype=np.float32), rolling_windows)
        metrics["ContextLen"] = clen
        metrics["EvalSamples"] = len(actuals)
        all_stats.append(metrics)

    if not all_stats:
        print(f"没有可用回测样本（可能切分窗口过窄）：{symbol}")
        return None

    stats_df = pd.DataFrame(all_stats)
    print("\n回测结果汇总:")
    print(stats_df.to_string(index=False))

    best_clen = stats_df.loc[stats_df["RMSE"].idxmin(), "ContextLen"]
    print(f"\n建议：对于 {symbol}，最优上下文长度为 {int(best_clen)} (基于最小 RMSE)。")

    return stats_df


def main() -> None:
    parser = argparse.ArgumentParser(description="TimesFM 滚动回测工具")
    parser.add_argument("--symbol", type=str, required=True, help="股票代码")
    parser.add_argument("--provider", type=str, default="akshare", help="数据源")
    parser.add_argument("--start", type=str, default="2023-01-01", help="开始日期")
    parser.add_argument("--end", type=str, default=None, help="结束日期")
    parser.add_argument("--test-days", type=int, default=20, help="回测天数")
    parser.add_argument("--horizon", type=int, default=5, help="预测步长")
    parser.add_argument("--adapter", type=str, help="适配器路径 (可选)")
    parser.add_argument("--input-csv", type=str, help="本地 CSV 数据路径 (配合 --provider local)")
    parser.add_argument("--duckdb-path", type=str, help="DuckDB 文件路径 (market.duckdb)")
    parser.add_argument("--train-end", type=str, default=None, help="训练截止日 (YYYY-MM-DD)")
    parser.add_argument("--test-start", type=str, default=None, help="测试开始日 (YYYY-MM-DD)")
    parser.add_argument("--test-end", type=str, default=None, help="测试结束日 (YYYY-MM-DD)")
    parser.add_argument("--rolling-windows", type=str, default="20,40,60", help="recent 指标窗口")

    args = parser.parse_args()

    context_lengths = [30, 60, 90, 128, 256, 512]
    rolling_windows = [int(x.strip()) for x in args.rolling_windows.split(",") if x.strip()]
    run_backtest(
        symbol=args.symbol,
        provider=args.provider,
        start_date=args.start,
        end_date=args.end,
        context_lengths=context_lengths,
        horizon=args.horizon,
        test_days=args.test_days,
        adapter_path=args.adapter,
        input_csv=args.input_csv,
        duckdb_path=args.duckdb_path,
        train_end_date=args.train_end,
        test_start_date=args.test_start,
        test_end_date=args.test_end,
        rolling_windows=rolling_windows,
    )


if __name__ == "__main__":
    main()
