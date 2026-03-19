"""
指数成份股宇宙模块 (Index Stock Universe)

提供从 AkShare 拉取各类指数成份股并持久化至 DuckDB 的能力。

Usage:
    from timesfm_cn_forecast.universe import get_stock_universe
    stocks = get_stock_universe('CYBZ', duckdb_path='data/index_market.duckdb')
    # -> ['300015', '300024', ...]
"""
from __future__ import annotations

import json
from pathlib import Path

from .fetcher import fetch_constituents, INDEX_MAP
from .storage import upsert_constituents, query_constituents

__all__ = [
    "fetch_constituents",
    "query_constituents",
    "upsert_constituents",
    "get_stock_universe",
    "INDEX_MAP",
]


def _normalize_code(code: str) -> str:
    return "".join(ch for ch in str(code) if ch.isdigit()).zfill(6)


def _load_dynamic_group(index_symbol: str, group_definitions_dir: str) -> list[str]:
    base = Path(group_definitions_dir)
    if not base.exists():
        return []

    for file in sorted(base.glob("*.json")):
        try:
            payload = json.loads(file.read_text(encoding="utf-8"))
        except Exception:
            continue

        if isinstance(payload, dict) and isinstance(payload.get("groups"), list):
            for item in payload["groups"]:
                if not isinstance(item, dict):
                    continue
                if str(item.get("name", "")).strip() != index_symbol:
                    continue
                syms = item.get("symbols", [])
                return [_normalize_code(sym) for sym in syms if str(sym).strip()]

        if isinstance(payload, dict) and isinstance(payload.get(index_symbol), list):
            return [_normalize_code(sym) for sym in payload[index_symbol] if str(sym).strip()]

    return []


def get_stock_universe(
    index_symbol: str,
    duckdb_path: str,
    group_definitions_dir: str = "data/group_definitions",
) -> list[str]:
    """
    获取指定分组的股票池，解析优先级：
    1. single_<symbol>
    2. DuckDB 现有分组
    3. data/group_definitions/*.json 动态分组

    Args:
        index_symbol: 逻辑指数代号，如 'CYBZ', 'HS300', 'ZZ500'。
        duckdb_path: index_market.duckdb 文件路径。
        group_definitions_dir: 动态分组 JSON 目录。

    Returns:
        股票代码列表（6位纯数字）。
    """
    normalized = str(index_symbol).strip()
    if normalized.startswith("single_"):
        symbol = _normalize_code(normalized.split("single_", 1)[1])
        return [symbol] if symbol and symbol != "000000" else []

    stocks = query_constituents(normalized, duckdb_path)
    if stocks:
        return [_normalize_code(stock) for stock in stocks]

    dynamic_stocks = _load_dynamic_group(normalized, group_definitions_dir)
    if dynamic_stocks:
        return dynamic_stocks

    return []
