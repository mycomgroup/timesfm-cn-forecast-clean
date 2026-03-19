from __future__ import annotations

import pandas as pd


def compute_alpha_returns(
    stock_df: pd.DataFrame,
    index_df: pd.DataFrame,
    horizon: int = 1,
    price_col: str = "value",
) -> pd.DataFrame:
    """计算个股相对指数的超额收益（alpha）。"""
    if "date" not in stock_df.columns:
        stock_df = stock_df.reset_index().rename(columns={stock_df.index.name or "index": "date"})
    if "date" not in index_df.columns:
        index_df = index_df.reset_index().rename(columns={index_df.index.name or "index": "date"})

    s = stock_df[["date", price_col]].copy()
    i = index_df[["date", price_col]].copy()
    s["date"] = pd.to_datetime(s["date"])
    i["date"] = pd.to_datetime(i["date"])

    s = s.sort_values("date").rename(columns={price_col: "stock_price"})
    i = i.sort_values("date").rename(columns={price_col: "index_price"})

    merged = pd.merge(s, i, on="date", how="inner")
    merged[f"stock_ret_{horizon}d"] = merged["stock_price"].shift(-horizon) / merged["stock_price"] - 1.0
    merged[f"index_ret_{horizon}d"] = merged["index_price"].shift(-horizon) / merged["index_price"] - 1.0
    merged[f"alpha_{horizon}d"] = merged[f"stock_ret_{horizon}d"] - merged[f"index_ret_{horizon}d"]
    return merged.dropna().reset_index(drop=True)
