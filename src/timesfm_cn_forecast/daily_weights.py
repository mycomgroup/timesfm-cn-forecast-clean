from __future__ import annotations

import pandas as pd


def build_daily_weights(df: pd.DataFrame, top_k: int = 20) -> pd.DataFrame:
    """根据 signal_score 和稳定性输出当日权重。"""
    required = {"date", "group", "symbol", "signal_score", "expected_return", "recent_stability"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")

    use = df.copy()
    use["rank_score"] = use["signal_score"] * use["recent_stability"]
    use = use.sort_values("rank_score", ascending=False).head(top_k)
    denom = use["rank_score"].clip(lower=0).sum()
    if denom <= 1e-8:
        use["weight"] = 0.0
    else:
        use["weight"] = use["rank_score"].clip(lower=0) / denom
    return use[["date", "group", "symbol", "signal_score", "expected_return", "recent_stability", "weight"]]
