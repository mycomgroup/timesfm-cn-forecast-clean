#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""根据种子股候选，构造精炼陪跑组定义。"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
import sys
from typing import Iterable

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from timesfm_cn_forecast.providers import DataRequest, load_historical_data


CANONICAL_ALIASES: dict[str, list[str]] = {
    "group_name": ["group_name", "group"],
    "hitrate": ["hitrate", "HitRate"],
    "profit_factor": ["profit_factor", "ProfitFactor"],
    "max_drawdown": ["max_drawdown", "MaxDrawdown"],
    "recent20_avg_ret": ["recent20_avg_ret", "Recent20AvgRet"],
    "recent60_avg_ret": ["recent60_avg_ret", "Recent60AvgRet"],
    "trade_score": ["trade_score"],
}


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

    by_group_name: dict[str, Path] = {}
    for path in by_parent.values():
        gname = path.parent.name
        current = by_group_name.get(gname)
        if current is None or path.stat().st_mtime > current.stat().st_mtime:
            by_group_name[gname] = path
    return sorted(by_group_name.values())


def _first_existing_column(columns: Iterable[str], candidates: list[str]) -> str | None:
    colset = set(columns)
    for col in candidates:
        if col in colset:
            return col
    return None


def _canonicalize_result_df(df: pd.DataFrame, group_name: str) -> pd.DataFrame:
    out = df.copy()
    if "symbol" not in out.columns:
        return pd.DataFrame()

    out["symbol"] = out["symbol"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    out["group_name"] = group_name
    if "status" not in out.columns:
        out["status"] = "ok"

    for canonical, aliases in CANONICAL_ALIASES.items():
        if canonical in out.columns:
            continue
        src = _first_existing_column(out.columns, aliases)
        if src is not None:
            out[canonical] = out[src]
    for col in ["hitrate", "profit_factor", "max_drawdown", "recent20_avg_ret", "recent60_avg_ret", "trade_score"]:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if out["trade_score"].isna().all():
        ok = out["status"] == "ok"
        out.loc[ok, "trade_score"] = _compute_trade_score(out.loc[ok])
    return out


def _load_results(input_dir: Path) -> pd.DataFrame:
    frames = []
    for path in _pick_result_files(input_dir):
        group_name = path.parent.name
        df = pd.read_csv(path, low_memory=False)
        cdf = _canonicalize_result_df(df, group_name=group_name)
        if cdf.empty:
            continue
        frames.append(cdf)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _load_top3(top3_path: Path) -> pd.DataFrame:
    if not top3_path.exists():
        return pd.DataFrame(columns=["group_name", "symbol", "group_rank"])
    df = pd.read_csv(top3_path, low_memory=False)
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    if "group_name" not in df.columns and "group" in df.columns:
        df["group_name"] = df["group"]
    if "group_rank" not in df.columns:
        df["group_rank"] = np.nan
    return df


def _load_price_matrix(symbols: list[str], duckdb_path: str, start: str, end: str | None) -> pd.DataFrame:
    frames: list[pd.Series] = []
    for symbol in symbols:
        req = DataRequest(
            provider="duckdb",
            symbol=symbol,
            start=start,
            end=end,
            value_column="close",
            duckdb_path=duckdb_path,
        )
        try:
            df = load_historical_data(req)
        except Exception:
            continue
        if df.empty:
            continue
        s = df["value"].astype(float).copy()
        s.name = symbol
        frames.append(s)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=1).sort_index().ffill().bfill()


def _build_seed_pool(
    seed: str,
    results_df: pd.DataFrame,
    top3_df: pd.DataFrame,
    initial_pool_size: int,
) -> pd.DataFrame:
    ok_df = results_df[results_df["status"] == "ok"].copy()
    if ok_df.empty:
        return pd.DataFrame(columns=["symbol", "coappear_count", "avg_group_trade_score"])

    groups_seed_top3 = set(top3_df.loc[top3_df["symbol"] == seed, "group_name"].dropna().astype(str).tolist())
    if not groups_seed_top3:
        groups_seed_top3 = set(ok_df.loc[ok_df["symbol"] == seed, "group_name"].astype(str).tolist())

    pool_rows = []
    for group_name, gdf in ok_df.groupby("group_name"):
        if group_name not in groups_seed_top3:
            continue
        if seed not in set(gdf["symbol"].tolist()):
            continue
        ranked = gdf.sort_values(
            by=["trade_score", "recent20_avg_ret", "hitrate"],
            ascending=[False, False, False],
            na_position="last",
        ).head(max(initial_pool_size, 20))
        peers = ranked[ranked["symbol"] != seed]
        pool_rows.append(peers[["symbol", "trade_score"]].copy())

    if pool_rows:
        pool_df = pd.concat(pool_rows, ignore_index=True)
        agg = (
            pool_df.groupby("symbol")
            .agg(
                coappear_count=("symbol", "count"),
                avg_group_trade_score=("trade_score", "mean"),
            )
            .reset_index()
        )
    else:
        agg = pd.DataFrame(columns=["symbol", "coappear_count", "avg_group_trade_score"])

    # 如果跨组共现不足，退化为全局高分股补池
    if len(agg) < max(5, initial_pool_size // 2):
        fallback = (
            ok_df[ok_df["symbol"] != seed]
            .groupby("symbol")
            .agg(
                coappear_count=("symbol", "count"),
                avg_group_trade_score=("trade_score", "mean"),
            )
            .reset_index()
        )
        agg = (
            pd.concat([agg, fallback], ignore_index=True)
            .groupby("symbol", as_index=False)
            .agg(
                coappear_count=("coappear_count", "max"),
                avg_group_trade_score=("avg_group_trade_score", "max"),
            )
        )

    agg = agg.sort_values(
        by=["coappear_count", "avg_group_trade_score"],
        ascending=[False, False],
        na_position="last",
    ).head(max(initial_pool_size * 3, 60))
    return agg


def _score_companions(
    seed: str,
    pool_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    if pool_df.empty:
        return pool_df
    out = pool_df.copy()

    if seed not in price_df.columns:
        out["corr"] = np.nan
        out["sync"] = np.nan
        out["vol_similarity"] = np.nan
    else:
        rets = price_df.pct_change().replace([np.inf, -np.inf], np.nan).dropna(how="all")
        seed_ret = rets[seed] if seed in rets.columns else pd.Series(dtype=float)
        seed_std = float(seed_ret.std()) if not seed_ret.empty else np.nan
        corr_map = {}
        sync_map = {}
        vol_sim_map = {}
        for symbol in out["symbol"]:
            if symbol not in rets.columns:
                corr_map[symbol] = np.nan
                sync_map[symbol] = np.nan
                vol_sim_map[symbol] = np.nan
                continue
            peer = rets[symbol]
            corr = float(seed_ret.corr(peer)) if not seed_ret.empty else np.nan
            sync = float((np.sign(seed_ret) == np.sign(peer)).mean()) if not seed_ret.empty else np.nan
            peer_std = float(peer.std()) if not peer.empty else np.nan
            if np.isnan(seed_std) or np.isnan(peer_std):
                vol_sim = np.nan
            else:
                vol_sim = 1.0 - abs(peer_std - seed_std) / max(seed_std, 1e-8)
            corr_map[symbol] = corr
            sync_map[symbol] = sync
            vol_sim_map[symbol] = float(np.clip(vol_sim, 0.0, 1.0)) if not np.isnan(vol_sim) else np.nan

        out["corr"] = out["symbol"].map(corr_map)
        out["sync"] = out["symbol"].map(sync_map)
        out["vol_similarity"] = out["symbol"].map(vol_sim_map)

    out["coappear_score"] = (
        0.7 * _rank_pct(out["coappear_count"], ascending=False)
        + 0.3 * _rank_pct(out["avg_group_trade_score"], ascending=False)
    )
    out["corr_score"] = (pd.to_numeric(out["corr"], errors="coerce").fillna(0.0) + 1.0) / 2.0
    out["sync_score"] = pd.to_numeric(out["sync"], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    out["vol_similarity_score"] = pd.to_numeric(out["vol_similarity"], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    out["companion_score"] = (
        0.45 * out["coappear_score"]
        + 0.30 * out["corr_score"]
        + 0.15 * out["vol_similarity_score"]
        + 0.10 * out["sync_score"]
    )
    out = out.sort_values(
        by=["companion_score", "coappear_count", "corr"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)
    out["pool_rank"] = np.arange(1, len(out) + 1)
    return out


def _pick_variant_symbols(
    scored_df: pd.DataFrame,
    final_size: int,
    mode: str,
) -> list[str]:
    if scored_df.empty:
        return []

    if mode == "balanced":
        ranked = scored_df.sort_values(
            by=["companion_score", "coappear_count", "corr"],
            ascending=[False, False, False],
            na_position="last",
        )
    elif mode == "high_corr":
        ranked = scored_df.sort_values(
            by=["corr", "companion_score", "coappear_count"],
            ascending=[False, False, False],
            na_position="last",
        )
    elif mode == "stable_vol":
        ranked = scored_df.sort_values(
            by=["vol_similarity_score", "companion_score", "coappear_count"],
            ascending=[False, False, False],
            na_position="last",
        )
    else:
        ranked = scored_df

    symbols = ranked["symbol"].astype(str).head(max(final_size, 1)).tolist()
    deduped = []
    seen = set()
    for symbol in symbols:
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped.append(symbol)
    return deduped


def _save_seed_definition(
    seed: str,
    scored_df: pd.DataFrame,
    output_dir: Path,
    reduced_pool_size: int,
    final_size: int,
) -> tuple[Path, list[str]]:
    reduced = scored_df.head(max(reduced_pool_size, final_size)).copy()
    v1 = _pick_variant_symbols(reduced, final_size=final_size, mode="balanced")
    v2 = _pick_variant_symbols(reduced, final_size=final_size, mode="high_corr")
    v3 = _pick_variant_symbols(reduced, final_size=final_size, mode="stable_vol")

    groups = []
    variants = [("v1", "balanced", v1), ("v2", "high_corr", v2), ("v3", "stable_vol", v3)]
    created_group_names = []
    for suffix, mode, peers in variants:
        if not peers:
            continue
        group_name = f"seed_{seed}_{suffix}"
        created_group_names.append(group_name)
        groups.append(
            {
                "name": group_name,
                "seed": seed,
                "variant": mode,
                "symbols": [seed] + peers,
                "meta": {
                    "seed_only_target": True,
                    "reduced_pool_size": int(reduced_pool_size),
                    "final_size": int(final_size),
                },
            }
        )

    payload = {
        "seed": seed,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "groups": groups,
    }

    output_path = output_dir / f"seed_{seed}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path, created_group_names


def main() -> None:
    parser = argparse.ArgumentParser(description="Build refined companion groups for seed stocks.")
    parser.add_argument(
        "--seed-candidates",
        type=str,
        default="data/tasks/supernode_summary/seed_candidates.csv",
        help="CSV path with seed candidates (requires symbol column)",
    )
    parser.add_argument(
        "--top3-by-group",
        type=str,
        default="data/tasks/supernode_summary/top3_by_group.csv",
        help="Optional top3-by-group CSV from summarize_supernodes.py",
    )
    parser.add_argument(
        "--group-results-dir",
        type=str,
        default="data/tasks",
        help="Root directory containing group result csv files",
    )
    parser.add_argument("--market-duckdb", type=str, default="data/market.duckdb")
    parser.add_argument("--start", type=str, default="2025-01-01")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--max-seeds", type=int, default=10)
    parser.add_argument("--initial-pool-size", type=int, default=50)
    parser.add_argument("--reduced-pool-size", type=int, default=20)
    parser.add_argument("--final-size", type=int, default=8)
    parser.add_argument("--output-dir", type=str, default="data/group_definitions")
    parser.add_argument("--analysis-output", type=str, default="data/tasks/supernode_summary")
    args = parser.parse_args()

    seed_path = Path(args.seed_candidates)
    if not seed_path.exists():
        raise FileNotFoundError(f"未找到种子候选文件: {seed_path}")

    seed_df = pd.read_csv(seed_path, low_memory=False)
    if "symbol" not in seed_df.columns:
        raise ValueError("seed_candidates.csv 必须包含 symbol 列")
    seed_df["symbol"] = seed_df["symbol"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    max_seeds = max(int(args.max_seeds), 1)
    seeds = seed_df["symbol"].dropna().astype(str).head(max_seeds).tolist()
    if not seeds:
        raise RuntimeError("seed_candidates.csv 没有可用 symbol")

    results_df = _load_results(Path(args.group_results_dir))
    if results_df.empty:
        raise RuntimeError(f"未在 {args.group_results_dir} 发现可用组结果")

    top3_df = _load_top3(Path(args.top3_by_group))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir = Path(args.analysis_output)
    analysis_dir.mkdir(parents=True, exist_ok=True)

    all_rankings = []
    index_rows = []
    for seed in seeds:
        pool_df = _build_seed_pool(
            seed=seed,
            results_df=results_df,
            top3_df=top3_df,
            initial_pool_size=max(int(args.initial_pool_size), 10),
        )
        if pool_df.empty:
            print(f"[WARN] {seed}: 无法构建候选池，跳过。")
            continue

        candidate_symbols = [seed] + pool_df["symbol"].astype(str).tolist()
        price_df = _load_price_matrix(
            symbols=candidate_symbols,
            duckdb_path=args.market_duckdb,
            start=args.start,
            end=args.end,
        )
        scored = _score_companions(seed=seed, pool_df=pool_df, price_df=price_df)
        scored = scored.head(max(int(args.initial_pool_size), 10))
        scored["seed"] = seed
        all_rankings.append(scored)

        json_path, group_names = _save_seed_definition(
            seed=seed,
            scored_df=scored,
            output_dir=output_dir,
            reduced_pool_size=max(int(args.reduced_pool_size), int(args.final_size)),
            final_size=max(int(args.final_size), 3),
        )
        index_rows.append(
            {
                "seed": seed,
                "json_path": str(json_path),
                "group_names": ",".join(group_names),
                "n_candidates": int(len(scored)),
            }
        )
        print(f"[OK] seed={seed} -> {json_path} ({len(group_names)} groups)")

    if all_rankings:
        ranking_path = analysis_dir / "seed_companion_pool.csv"
        pd.concat(all_rankings, ignore_index=True).to_csv(ranking_path, index=False)
        print(f"Companion ranking table saved to: {ranking_path}")

    if index_rows:
        index_path = analysis_dir / "seed_group_index.csv"
        pd.DataFrame(index_rows).to_csv(index_path, index=False)
        print(f"Seed group index saved to: {index_path}")


if __name__ == "__main__":
    main()
