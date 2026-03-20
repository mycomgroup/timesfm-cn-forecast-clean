"""
指数成份股 DuckDB 持久化模块。

负责将拉取后的成份股 DataFrame 写入 index_market.duckdb 的
`index_constituents` 表，并提供查询接口。
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS index_constituents (
    index_symbol  VARCHAR   NOT NULL,
    akshare_code  VARCHAR   NOT NULL,
    code          VARCHAR   NOT NULL,
    name          VARCHAR,
    in_date       DATE,
    fetched_at    TIMESTAMP,
    PRIMARY KEY (index_symbol, code)
);
"""


def _get_con(duckdb_path: str, read_only: bool = False):
    """获取 DuckDB 连接。"""
    try:
        import duckdb
    except ImportError as e:
        raise ImportError("请先安装 duckdb: pip install duckdb") from e

    path = Path(duckdb_path)
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def upsert_constituents(df: pd.DataFrame, duckdb_path: str) -> int:
    """
    将成份股 DataFrame 以 Upsert 方式写入 DuckDB。
    """
    if df.empty:
        return 0

    con = _get_con(duckdb_path, read_only=False)
    try:
        con.execute(_CREATE_TABLE_SQL)

        # 按 index_symbol 分组处理
        for sym, group_df in df.groupby("index_symbol"):
            con.execute(
                "DELETE FROM index_constituents WHERE index_symbol = ?", [sym]
            )
            con.register("_tmp_constituents", group_df)
            con.execute(
                "INSERT INTO index_constituents SELECT * FROM _tmp_constituents"
            )
            con.unregister("_tmp_constituents")
            logger.info(f"  [{sym}] 写入 {len(group_df)} 条成份股记录。")

        con.commit()
        return len(df)
    finally:
        con.close()


def query_constituents(index_symbol: str, duckdb_path: str) -> list[str]:
    """
    从 DuckDB 查询指定指数的成份股代码列表。
    """
    con = _get_con(duckdb_path, read_only=True)
    try:
        result = con.execute(
            "SELECT code FROM index_constituents WHERE index_symbol = ? ORDER BY code",
            [index_symbol],
        ).fetchall()
        return [row[0] for row in result]
    except Exception as err:
        logger.warning(f"查询失败: {err}")
        return []
    finally:
        con.close()


def get_index_constituents(index_symbol: str, duckdb_path: str) -> pd.DataFrame:
    """返回指定分组的完整成份股快照。"""
    con = _get_con(duckdb_path, read_only=True)
    try:
        return con.execute(
            """
            SELECT index_symbol, akshare_code, code, name, in_date, fetched_at
            FROM index_constituents
            WHERE index_symbol = ?
            ORDER BY code
            """,
            [index_symbol],
        ).fetchdf()
    except Exception as err:
        logger.warning(f"读取分组快照失败: {err}")
        return pd.DataFrame(
            columns=["index_symbol", "akshare_code", "code", "name", "in_date", "fetched_at"]
        )
    finally:
        con.close()


def list_all_symbols(duckdb_path: str) -> pd.DataFrame:
    """
    列出 DuckDB 中已存储的所有指数及其成份股数量。
    """
    con = _get_con(duckdb_path, read_only=True)
    try:
        return con.execute("""
            SELECT index_symbol, COUNT(*) as count, MAX(fetched_at) as fetched_at
            FROM index_constituents
            GROUP BY index_symbol
            ORDER BY index_symbol
        """).fetchdf()
    except Exception:
        return pd.DataFrame(columns=["index_symbol", "count", "fetched_at"])
    finally:
        con.close()
