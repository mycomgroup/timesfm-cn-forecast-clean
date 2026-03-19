#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""汇总分组评估结果，筛选超级节点种子股。"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


CANONICAL_ALIASES: dict[str, list[str]] = {
    "group_name": ["group_name", "group"],
    "hitrate": ["hitrate", "HitRate"],
    "rmse": ["rmse", "RMSE"],
    "mae": ["mae", "MAE"],
    "mape": ["mape", "MAPE"],
    "avg_ret": ["avg_ret", "AvgRet"],
    "cum_ret": ["cum_ret", "CumRet"],
    "profit_factor": ["profit_factor", "ProfitFactor"],
    "win_loss_ratio": ["win_loss_ratio", "WinLossRatio"],
    "max_drawdown": ["max_drawdown", "MaxDrawdown"],
    "recent20_avg_ret": ["recent20_avg_ret", "Recent20AvgRet"],
    "recent40_avg_ret": ["recent40_avg_ret", "Recent40AvgRet"],
    "recent60_avg_ret": ["recent60_avg_ret", "Recent60AvgRet"],
    "trade_score": ["trade_score"],
    "recent_rank": ["recent_rank"],
    "supernode_candidate": ["supernode_candidate"],
}

NUMERIC_COLUMNS = [
    "hitrate",
    "rmse",
    "mae",
    "mape",
    "avg_ret",
    "cum_ret",
    "profit_factor",
    "win_loss_ratio",
    "max_drawdown",
    "recent20_avg_ret",
    "recent40_avg_ret",
    "recent60_avg_ret",
    "trade_score",
    "recent_rank",
]


def _rank_pct(series: pd.Series, ascending: bool) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)
    if vals.notna().sum() == 0:
        return pd.Series(0.0, index=series.index, dtype=float)
    fill_val = float(vals.median()) if vals.notna().any() else 0.0
    vals = vals.fillna(fill_val)
    if vals.nunique(dropna=True) <= 1:
        return pd.Series(0.5, index=series.index, dtype=float)
    return vals.rank(method="average", ascending=ascending, pct=True).astype(float)


def _compute_trade_score(df: pd.DataFrame) -> pd.Series:
    s_recent20 = _rank_pct(df["recent20_avg_ret"], ascending=False)
    s_recent60 = _rank_pct(df["recent60_avg_ret"], ascending=False)
    s_hitrate = _rank_pct(df["hitrate"], ascending=False)
    s_pf = _rank_pct(df["profit_factor"], ascending=False)
    s_mdd = _rank_pct(pd.to_numeric(df["max_drawdown"], errors="coerce").abs(), ascending=True)
    return (
        0.40 * s_recent20
        + 0.20 * s_recent60
        + 0.20 * s_hitrate
        + 0.10 * s_pf
        - 0.10 * s_mdd
    )


def _first_existing_column(columns: Iterable[str], candidates: list[str]) -> str | None:
    colset = set(columns)
    for col in candidates:
        if col in colset:
            return col
    return None


def _canonicalize(df: pd.DataFrame, group_name: str) -> pd.DataFrame:
    out = df.copy()
    if "symbol" not in out.columns:
        return pd.DataFrame()

    out["symbol"] = out["symbol"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    out["group_name"] = group_name

    for canonical, aliases in CANONICAL_ALIASES.items():
        if canonical in out.columns:
            continue
        src = _first_existing_column(out.columns, aliases)
        if src is not None:
            out[canonical] = out[src]

    if "status" not in out.columns:
        out["status"] = "ok"

    for col in NUMERIC_COLUMNS:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")

    if "supernode_candidate" not in out.columns:
        out["supernode_candidate"] = False
    out["supernode_candidate"] = out["supernode_candidate"].fillna(False).astype(bool)
    return out


def _pick_result_files(input_dir: Path) -> list[Path]:
    by_parent: dict[Path, Path] = {}

    for path in input_dir.rglob("group_full_results.csv"):
        by_parent[path.parent] = path

    for path in sorted(input_dir.rglob("results_*.csv")):
        if path.parent in by_parent:
            continue
        current = by_parent.get(path.parent)
        if current is None or path.stat().st_mtime > current.stat().st_mtime:
            by_parent[path.parent] = path

    # 同名分组可能在多个 task 下重复出现，默认保留最新的一份。
    by_group_name: dict[str, Path] = {}
    for path in by_parent.values():
        gname = path.parent.name
        current = by_group_name.get(gname)
        if current is None or path.stat().st_mtime > current.stat().st_mtime:
            by_group_name[gname] = path
    return sorted(by_group_name.values())


def _is_allowed_group(group_name: str, prefixes: list[str]) -> bool:
    if not prefixes:
        return True
    return any(group_name.startswith(prefix) for prefix in prefixes)


def _build_topk_by_group(
    full_df: pd.DataFrame,
    topk: int,
    hitrate_min: float,
    recent20_min: float,
    profit_factor_min: float,
) -> pd.DataFrame:
    rows = []
    for group, gdf in full_df.groupby("group_name"):
        ok_df = gdf[gdf["status"] == "ok"].copy()
        if ok_df.empty:
            continue

        if ok_df["trade_score"].isna().all():
            ok_df["trade_score"] = _compute_trade_score(ok_df)
        if ok_df["recent_rank"].isna().all():
            ok_df["recent_rank"] = (
                ok_df["recent20_avg_ret"].rank(method="dense", ascending=False).astype("Int64")
            )
        ok_df["supernode_candidate"] = (
            (ok_df["hitrate"] >= hitrate_min)
            & (ok_df["recent20_avg_ret"] > recent20_min)
            & (ok_df["profit_factor"] > profit_factor_min)
        )

        ranked = ok_df.sort_values(
            by=["trade_score", "recent20_avg_ret", "hitrate", "profit_factor"],
            ascending=[False, False, False, False],
            na_position="last",
        ).head(topk)
        ranked = ranked.copy()
        ranked["group_rank"] = np.arange(1, len(ranked) + 1)
        rows.append(ranked)

    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _build_cross_group_rank(top_df: pd.DataFrame) -> pd.DataFrame:
    if top_df.empty:
        return pd.DataFrame()

    ranked = top_df.copy()
    grouped = ranked.groupby("symbol")
    cross = grouped.agg(
        appear_count_top3=("group_name", "nunique"),
        appear_count_top1=("group_rank", lambda x: int((x == 1).sum())),
        best_hitrate=("hitrate", "max"),
        avg_hitrate=("hitrate", "mean"),
        best_recent20_avg_ret=("recent20_avg_ret", "max"),
        avg_recent20_avg_ret=("recent20_avg_ret", "mean"),
        best_trade_score=("trade_score", "max"),
        avg_trade_score=("trade_score", "mean"),
    ).reset_index()

    group_map = grouped["group_name"].apply(lambda x: ",".join(sorted(set(x)))).rename("groups")
    top1_map = (
        ranked[ranked["group_rank"] == 1]
        .groupby("symbol")["group_name"]
        .apply(lambda x: ",".join(sorted(set(x))))
        .rename("top1_groups")
    )
    cross = cross.merge(group_map, on="symbol", how="left")
    cross = cross.merge(top1_map, on="symbol", how="left")

    cross = cross.sort_values(
        by=["appear_count_top3", "appear_count_top1", "avg_trade_score", "avg_recent20_avg_ret"],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)
    cross["cross_group_rank"] = np.arange(1, len(cross) + 1)
    return cross


def _build_seed_candidates(
    cross_df: pd.DataFrame,
    min_repeat_top3: int,
    hitrate_min: float,
    recent20_min: float,
    max_seeds: int,
) -> pd.DataFrame:
    if cross_df.empty:
        return pd.DataFrame()

    seeds = cross_df[
        (cross_df["appear_count_top3"] >= min_repeat_top3)
        & (cross_df["best_hitrate"] >= hitrate_min)
        & (cross_df["best_recent20_avg_ret"] > recent20_min)
    ].copy()

    if seeds.empty:
        fallback_n = max_seeds if max_seeds > 0 else 10
        seeds = cross_df.head(fallback_n).copy()
        seeds["selected_reason"] = "fallback_top_rank"
    else:
        seeds["selected_reason"] = "repeat_and_recent_positive"

    if max_seeds > 0:
        seeds = seeds.head(max_seeds)

    seeds["seed_rank"] = np.arange(1, len(seeds) + 1)
    return seeds


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize supernodes from group evaluation results.")
    parser.add_argument("--input-dir", type=str, default="data/tasks", help="Root dir containing group results")
    parser.add_argument(
        "--group-prefixes",
        type=str,
        default="ind_,con_,resonance_,vol_",
        help="Only include groups with these prefixes (comma-separated). Empty means include all.",
    )
    parser.add_argument("--topk-per-group", type=int, default=3)
    parser.add_argument("--min-repeat-top3", type=int, default=2)
    parser.add_argument("--hitrate-min", type=float, default=51.0)
    parser.add_argument("--recent20-min", type=float, default=0.0)
    parser.add_argument("--profit-factor-min", type=float, default=1.0)
    parser.add_argument("--max-seeds", type=int, default=20)
    parser.add_argument("--output-dir", type=str, default="data/tasks/supernode_summary")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prefixes = [p.strip() for p in args.group_prefixes.split(",") if p.strip()]
    files = _pick_result_files(input_dir)
    if not files:
        raise FileNotFoundError(f"未找到组结果文件: {input_dir}")

    all_frames = []
    for path in files:
        group_name = path.parent.name
        if not _is_allowed_group(group_name, prefixes):
            continue
        df = pd.read_csv(path, low_memory=False)
        cdf = _canonicalize(df, group_name=group_name)
        if cdf.empty:
            continue
        cdf["source_file"] = str(path)
        all_frames.append(cdf)

    if not all_frames:
        raise RuntimeError("没有可用分组结果（可能都被 group-prefixes 过滤了）。")

    full_df = pd.concat(all_frames, ignore_index=True)
    top3_df = _build_topk_by_group(
        full_df=full_df,
        topk=max(int(args.topk_per_group), 1),
        hitrate_min=args.hitrate_min,
        recent20_min=args.recent20_min,
        profit_factor_min=args.profit_factor_min,
    )
    cross_df = _build_cross_group_rank(top3_df)
    seeds_df = _build_seed_candidates(
        cross_df=cross_df,
        min_repeat_top3=max(int(args.min_repeat_top3), 1),
        hitrate_min=args.hitrate_min,
        recent20_min=args.recent20_min,
        max_seeds=max(int(args.max_seeds), 0),
    )

    top3_path = output_dir / "top3_by_group.csv"
    cross_path = output_dir / "cross_group_repeat_rank.csv"
    seed_path = output_dir / "seed_candidates.csv"

    top3_df.to_csv(top3_path, index=False)
    cross_df.to_csv(cross_path, index=False)
    seeds_df.to_csv(seed_path, index=False)

    print(f"Top3 by group saved to: {top3_path}")
    print(f"Cross-group repeat rank saved to: {cross_path}")
    print(f"Seed candidates saved to: {seed_path}")


if __name__ == "__main__":
    main()
