#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .providers import DataRequest, load_historical_data


def _read_prices(symbols: list[str], duckdb_path: str, start: str, end: str) -> pd.DataFrame:
    frames = []
    for symbol in symbols:
        req = DataRequest(provider="duckdb", symbol=symbol, start=start, end=end, duckdb_path=duckdb_path)
        try:
            df = load_historical_data(req)
        except Exception:
            continue
        if df.empty:
            continue
        s = df[["value"]].copy()
        s.columns = [symbol]
        frames.append(s)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=1).sort_index().ffill().bfill()


def build_volatility_groups(price_df: pd.DataFrame) -> dict[str, list[str]]:
    rets = price_df.pct_change().dropna(how="all")
    vol = rets.std().sort_values()
    n = len(vol)
    if n == 0:
        return {}
    q1, q2, q3 = int(n * 0.25), int(n * 0.5), int(n * 0.75)
    return {
        "vol_low": vol.index[:q1].tolist(),
        "vol_mid": vol.index[q1:q3].tolist(),
        "vol_high": vol.index[q3:].tolist(),
        "vol_spike": vol.sort_values(ascending=False).index[: max(5, n // 10)].tolist(),
    }


def build_resonance_groups(price_df: pd.DataFrame, seeds: list[str], topk: int = 50) -> dict[str, list[str]]:
    rets = price_df.pct_change().dropna(how="all")
    corr = rets.corr().fillna(0.0)
    out: dict[str, list[str]] = {}
    for seed in seeds:
        if seed not in corr.columns:
            continue
        peers = corr[seed].drop(index=seed, errors="ignore").sort_values(ascending=False).head(topk)
        out[f"resonance_{seed}"] = peers.index.tolist()
    return out


def build_seed_expansion_groups(price_df: pd.DataFrame, seeds: list[str], topk: int = 50) -> dict[str, list[str]]:
    return build_resonance_groups(price_df, seeds, topk=topk)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build dynamic group definitions")
    parser.add_argument("--symbols-file", type=str, required=True, help="CSV with column symbol")
    parser.add_argument("--duckdb-path", type=str, required=True)
    parser.add_argument("--start", type=str, default="2024-01-01")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--seed-file", type=str, default=None, help="Text file, one seed per line")
    parser.add_argument("--output-dir", type=str, default="data/group_definitions")
    args = parser.parse_args()

    symbols = pd.read_csv(args.symbols_file)["symbol"].astype(str).str.zfill(6).tolist()
    seeds = symbols[:20]
    if args.seed_file and Path(args.seed_file).exists():
        seeds = [line.strip() for line in Path(args.seed_file).read_text(encoding="utf-8").splitlines() if line.strip()]

    price_df = _read_prices(symbols, args.duckdb_path, args.start, args.end)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    vol_groups = build_volatility_groups(price_df)
    resonance_groups = build_resonance_groups(price_df, seeds)
    seed_groups = build_seed_expansion_groups(price_df, seeds)

    (out_dir / "volatility_groups.json").write_text(json.dumps(vol_groups, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "resonance_groups.json").write_text(json.dumps(resonance_groups, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "seed_groups.json").write_text(json.dumps(seed_groups, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote dynamic group definitions to {out_dir}")


if __name__ == "__main__":
    main()
