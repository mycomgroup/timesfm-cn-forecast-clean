"""
指数成份股拉取模块。

支持三种数据来源:
  - "akshare": 通过 AkShare 的 index_stock_cons 接口拉取
  - "industry_csv": 从本地 industry_category.csv 按申万三级行业名过滤
  - "concept_csv": 从本地 concept_category.csv 按概念名过滤
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# 默认本地 CSV 路径（相对于项目根目录）
_DEFAULT_INDUSTRY_CSV = "data/industry_category.csv"
_DEFAULT_CONCEPT_CSV = "data/concept_category.csv"

# ---------------------------------------------------------------------------
# INDEX_MAP
# 每条记录必须包含:
#   source: "akshare" | "industry_csv" | "concept_csv"
#   description: 可读说明
# AkShare 类型额外需要:
#   codes: List[str]               AkShare 查询代码列表
#   prefix_filter: List[str]       要过滤的股票代码前缀
# CSV 类型额外需要:
#   category: str                  CSV 中的分类名（精确匹配）
#   prefix_filter: List[str]       要过滤的股票代码前缀
# ---------------------------------------------------------------------------
INDEX_MAP: dict[str, dict] = {
    # ── AkShare 宽基指数 ──────────────────────────────────────────────────────
    "HS300": {
        "source": "akshare",
        "codes": ["000300"],
        "prefix_filter": [],
        "description": "沪深300",
    },
    "ZZ500": {
        "source": "akshare",
        "codes": ["399905"],
        "prefix_filter": [],
        "description": "中证500",
    },
    "ZZ800": {
        "source": "akshare",
        "codes": ["399906"],
        "prefix_filter": [],
        "description": "中证800",
    },
    "CYBZ": {
        "source": "akshare",
        "codes": ["399006"],
        "prefix_filter": [],
        "description": "创业板指",
    },
    "ZXBZ": {
        "source": "akshare",
        "codes": ["399005"],
        "prefix_filter": [],
        "description": "中小板指",
    },
    "small": {
        "source": "akshare",
        "codes": ["399101"],
        "prefix_filter": ["68", "4", "8"],
        "description": "中小盘综指（过滤科创/北交所）",
    },
    "small_25": {
        "source": "akshare",
        "codes": ["000002", "399107"],
        "prefix_filter": [],
        "limit": 50,
        "description": "自定义 small_25（临时：未按市值筛选，仅取前 50）",
    },
    "A": {
        "source": "akshare",
        "codes": ["000002", "399107"],
        "prefix_filter": ["68", "4", "8"],
        "description": "全A（沪深，过滤科创/北交所）",
    },
    "AA": {
        "source": "akshare",
        "codes": ["000985"],
        "prefix_filter": ["3", "68", "4", "8"],
        "description": "全市场综指（过滤创业板/科创/北交所）",
    },
    "small_fengzhi": {
        "source": "akshare",
        "codes": ["000002", "399107"],
        "prefix_filter": ["3", "68", "4", "8"],
        "description": "自定义中小盘（沪深主板，过滤创业板/科创/北交所）",
    },
    "test_dry_run": {
        "source": "akshare",
        "codes": ["000300"],
        "prefix_filter": [],
        "limit": 2,
        "description": "Dry run test group (2 stocks)",
    },
    # ── 申万三级行业分组（来自 industry_category.csv） ────────────────────────
    "ind_消费电子": {
        "source": "industry_csv",
        "category": "消费电子零部件及组装III",
        "prefix_filter": [],
        "description": "申万行业：消费电子零部件及组装",
    },
    "ind_军工电子": {
        "source": "industry_csv",
        "category": "军工电子III",
        "prefix_filter": [],
        "description": "申万行业：军工电子",
    },
    "ind_芯片": {
        "source": "industry_csv",
        "category": "数字芯片设计III",
        "prefix_filter": [],
        "description": "申万行业：数字芯片设计",
    },
    "ind_IT服务": {
        "source": "industry_csv",
        "category": "IT服务III",
        "prefix_filter": [],
        "description": "申万行业：IT服务",
    },
    "ind_医疗耗材": {
        "source": "industry_csv",
        "category": "医疗耗材III",
        "prefix_filter": [],
        "description": "申万行业：医疗耗材",
    },
    "ind_化学制剂": {
        "source": "industry_csv",
        "category": "化学制剂III",
        "prefix_filter": [],
        "description": "申万行业：化学制剂",
    },
    "ind_汽车底盘": {
        "source": "industry_csv",
        "category": "底盘与发动机系统III",
        "prefix_filter": [],
        "description": "申万行业：底盘与发动机系统",
    },
    "ind_化工": {
        "source": "industry_csv",
        "category": "其他化学制品III",
        "prefix_filter": [],
        "description": "申万行业：其他化学制品",
    },
    "ind_半导体设备": {
        "source": "industry_csv",
        "category": "半导体设备III",
        "prefix_filter": [],
        "description": "申万行业：半导体设备",
    },
    "ind_软件开发": {
        "source": "industry_csv",
        "category": "横向通用软件III",
        "prefix_filter": [],
        "description": "申万行业：横向通用软件",
    },
    "ind_影视院线": {
        "source": "industry_csv",
        "category": "影视动漫制作III",
        "prefix_filter": [],
        "description": "申万行业：影视动漫",
    },
    "ind_白酒": {
        "source": "industry_csv",
        "category": "白酒III",
        "prefix_filter": [],
        "description": "申万行业：白酒",
    },
    "ind_乘用车": {
        "source": "industry_csv",
        "category": "综合乘用车III",
        "prefix_filter": [],
        "description": "申万行业：乘用车",
    },
    "ind_电池": {
        "source": "industry_csv",
        "category": "锂电池III",
        "prefix_filter": [],
        "description": "申万行业：锂电池",
    },
    # ── 概念分组（来自 concept_category.csv）──────────────────────────────────
    "con_低空经济": {
        "source": "concept_csv",
        "category": "低空经济",
        "prefix_filter": [],
        "description": "概念：低空经济",
    },
    "con_比亚迪链": {
        "source": "concept_csv",
        "category": "比亚迪概念",
        "prefix_filter": [],
        "description": "概念：比亚迪产业链",
    },
    "con_汽车零部件": {
        "source": "concept_csv",
        "category": "汽车零部件概念",
        "prefix_filter": [],
        "description": "概念：汽车零部件",
    },
    "con_军民融合": {
        "source": "concept_csv",
        "category": "军民融合",
        "prefix_filter": [],
        "description": "概念：军民融合",
    },
    "con_信创": {
        "source": "concept_csv",
        "category": "信创",
        "prefix_filter": [],
        "description": "概念：信创（国产软件）",
    },
    "con_氢能源": {
        "source": "concept_csv",
        "category": "氢能源",
        "prefix_filter": [],
        "description": "概念：氢能源",
    },
    "con_新能源": {
        "source": "concept_csv",
        "category": "新能源",
        "prefix_filter": [],
        "description": "概念：新能源",
    },
    "con_人工智能": {
        "source": "concept_csv",
        "category": "人工智能大模型",
        "prefix_filter": [],
        "description": "概念：人工智能大模型",
    },
    "con_算力租赁": {
        "source": "concept_csv",
        "category": "算力租赁",
        "prefix_filter": [],
        "description": "概念：算力中心/租赁",
    },
    "con_固态电池": {
        "source": "concept_csv",
        "category": "固态电池",
        "prefix_filter": [],
        "description": "概念：固态电池",
    },
    "con_英伟达概念": {
        "source": "concept_csv",
        "category": "英伟达概念",
        "prefix_filter": [],
        "description": "概念：英伟达产业链",
    },
    "con_华为概念": {
        "source": "concept_csv",
        "category": "华为鸿蒙",
        "prefix_filter": [],
        "description": "概念：华为鸿蒙",
    },
}

try:
    # 动态追加更多热门行业
    _extra_inds = [
        "股份制银行III", "房地产开发III", "电池化学品III", "显示器件III", "火电III", 
        "生猪养殖III", "通信网络设备及器件III", "证券III", "中药III", "风力发电III",
        "光伏辅材III", "电网自动化III", "医疗设备III", "工程机械整机III", "白色家电III"
    ]
    for _cat in _extra_inds:
        _key = f"ind_{_cat.replace('III', '')}"
        if _key not in INDEX_MAP:
            INDEX_MAP[_key] = {"source": "industry_csv", "category": _cat, "prefix_filter": [], "description": f"申万行业：{_cat}"}

    # 动态追加热门概念
    _extra_cons = [
        "储能", "机器人概念", "工业母机", "量子通信", "脑机接口", "飞行汽车(eVTOL)", 
        "虚拟现实", "跨境电商", "央企国企改革", "高股息100", "超级品牌", "工业4.0"
    ]
    for _cat in _extra_cons:
        _key = f"con_{_cat.replace('(eVTOL)', '')}"
        if _key not in INDEX_MAP:
            INDEX_MAP[_key] = {"source": "concept_csv", "category": _cat, "prefix_filter": [], "description": f"概念：{_cat}"}
except Exception as e:
    pass


# ---------------------------------------------------------------------------
# 数据源处理函数
# ---------------------------------------------------------------------------

def _normalize_code_6digit(raw_code: str) -> str:
    """将 AkShare 格式的 6 位纯数字代码规范化。"""
    return str(raw_code).strip().zfill(6)


def _xshare_to_dbsymbol(code: str) -> tuple[str, str]:
    """
    将 CSV 中的 '000001.XSHE' / '600001.XSHG' 格式转换为:
      - db_symbol: 'sz000001' / 'sh600001'（用于 market.duckdb 查询）
      - pure_code: '000001'（6 位纯数字）
    """
    parts = code.split(".")
    if len(parts) != 2:
        pure = str(code).strip().zfill(6)
        return pure, pure
    num, exchange = parts[0].strip(), parts[1].strip().upper()
    prefix = "sz" if exchange == "XSHE" else "sh"
    return prefix + num, num


def _fetch_from_akshare(index_symbol: str, cfg: dict) -> pd.DataFrame:
    """通过 AkShare 接口拉取宽基指数成份股。"""
    try:
        import akshare as ak
    except ImportError as e:
        raise ImportError("请先安装 akshare: pip install akshare") from e

    all_frames = []
    for akcode in cfg["codes"]:
        logger.info(f"  [{index_symbol}] AkShare code: {akcode}...")
        df = None
        try:
            df = ak.index_stock_cons(symbol=akcode)
            df = df.rename(columns={"品种代码": "code", "品种名称": "name", "纳入日期": "in_date"})
        except Exception as err:
            logger.warning(f"  [{index_symbol}] 拉取 {akcode} 默认接口失败: {err}，尝试 csindex...")
            try:
                # 尝试 csindex 备用接口 (例如 000300)
                df = ak.index_stock_cons_csindex(symbol=akcode)
                df = df.rename(columns={"成分券代码": "code", "成分券名称": "name", "日期": "in_date"})
            except Exception as err2:
                logger.warning(f"  [{index_symbol}] csindex 也失败: {err2}")
                pass
        
        if df is not None and not df.empty:
            if "code" in df.columns:
                df["code"] = df["code"].apply(_normalize_code_6digit)
            if "in_date" in df.columns:
                df["in_date"] = pd.to_datetime(df["in_date"], errors="coerce").dt.date
            else:
                df["in_date"] = None
            if "name" not in df.columns:
                df["name"] = ""
            df["akshare_code"] = akcode
            all_frames.append(df[["akshare_code", "code", "name", "in_date"]])

    if not all_frames:
        logger.warning(f"所有 AkShare 接口均失败: {index_symbol}，返回空数据框。")
        return pd.DataFrame(columns=["akshare_code", "code", "name", "in_date"])
    return pd.concat(all_frames, ignore_index=True)


def _fetch_from_csv(
    index_symbol: str,
    cfg: dict,
    csv_path: str,
    code_col: str = "code",
    name_col: str | None = None,
    category_col: str = "category",
) -> pd.DataFrame:
    """从本地 CSV 按 category 名过滤成份股。"""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV 文件不存在: {path}")

    cat_value = cfg["category"]
    df_all = pd.read_csv(path)
    df = df_all[df_all[category_col] == cat_value].copy()

    if df.empty:
        logger.warning(f"  [{index_symbol}] 在 {csv_path} 中未找到分类: {cat_value}")
        return pd.DataFrame(columns=["akshare_code", "code", "name", "in_date"])

    # 转换代码格式
    db_symbols, pure_codes = zip(*df[code_col].apply(_xshare_to_dbsymbol))
    df["code"] = list(pure_codes)
    df["akshare_code"] = cat_value      # 这里用 category 名作为 akshare_code 字段的占位
    df["in_date"] = None
    df["name"] = df[name_col].fillna("") if name_col and name_col in df.columns else ""

    logger.info(f"  [{index_symbol}] 从 CSV 读取 {len(df)} 条，分类: {cat_value}")
    return df[["akshare_code", "code", "name", "in_date"]]


# ---------------------------------------------------------------------------
# 主接口
# ---------------------------------------------------------------------------

def fetch_constituents(
    index_symbol: str,
    industry_csv: str = _DEFAULT_INDUSTRY_CSV,
    concept_csv: str = _DEFAULT_CONCEPT_CSV,
    duckdb_path: str = "data/index_market.duckdb",
) -> pd.DataFrame:
    """
    拉取指定逻辑指数/分组的所有成份股，规范化后返回 DataFrame。

    Args:
        index_symbol: 逻辑分组代号，如 'CYBZ', 'ind_消费电子', 'con_低空经济'。
        industry_csv: industry_category.csv 路径。
        concept_csv: concept_category.csv 路径。

    Returns:
        DataFrame with columns: [index_symbol, akshare_code, code, name, in_date, fetched_at]
    """
    if index_symbol not in INDEX_MAP:
        valid = list(INDEX_MAP.keys())
        raise ValueError(f"不支持的分组: {index_symbol}，可用选项: {valid}")

    cfg = INDEX_MAP[index_symbol]
    source = cfg.get("source", "akshare")

    merged = pd.DataFrame() # 初始化为空，防止未绑定异常
    try:
        if source == "akshare":
            merged = _fetch_from_akshare(index_symbol, cfg)
        elif source == "industry_csv":
            merged = _fetch_from_csv(
                index_symbol, cfg, industry_csv,
                code_col="code", name_col=None, category_col="category"
            )
        elif source == "concept_csv":
            merged = _fetch_from_csv(
                index_symbol, cfg, concept_csv,
                code_col="code", name_col="name", category_col="category"
            )
        else:
            raise ValueError(f"未知 source 类型: {source}")
    except Exception as fetch_err:
        logger.warning(f"  [{index_symbol}] 在线或本地 fetch_err 异常: {fetch_err}")

    # ===== 新增 DuckDB 后备熔断机制 =====
    # 如果此时 merged 还是为空（比如所有 API 真的全挂了），或者强制优先从本地读取
    # 为了保证健壮性，这里我们在拉取失败时自动降级到本地已有的全量旧数据
    if merged.empty and Path(duckdb_path).exists():
        logger.warning(f"  [{index_symbol}] 在线/CSV拉取获取到空数据，尝试降级读取本地已保存的 duckdb 缓存...")
        try:
            from timesfm_cn_forecast.universe.storage import get_index_constituents
            df_cache = get_index_constituents(index_symbol, duckdb_path=duckdb_path)
            if df_cache is not None and not df_cache.empty:
                logger.info(f"  [{index_symbol}] 成功从本地 duckdb 恢复 {len(df_cache)} 条历史快照数据。")
                # DataFrame 应该符合同样的字段定义
                return df_cache
        except Exception as e:
            logger.warning(f"  [{index_symbol}] 读取本地 duckdb 失败: {e}")
            pass

    # 去重
    merged = merged.drop_duplicates(subset=["code"]).reset_index(drop=True)

    # 前缀过滤
    prefix_filter = cfg.get("prefix_filter", [])
    if prefix_filter:
        before = len(merged)
        merged = merged[
            merged["code"].apply(lambda c: not any(c.startswith(p) for p in prefix_filter))
        ].reset_index(drop=True)
        logger.info(f"  [{index_symbol}] 前缀过滤: {before} -> {len(merged)} 只")

    # 可选：限制数量（如 small_25 临时取前 50）
    limit = cfg.get("limit")
    if isinstance(limit, int) and limit > 0:
        before = len(merged)
        merged = merged.head(limit).reset_index(drop=True)
        logger.info(f"  [{index_symbol}] 限制数量: {before} -> {len(merged)} 只")

    merged["index_symbol"] = index_symbol
    merged["fetched_at"] = datetime.now(tz=timezone.utc)
    logger.info(f"  [{index_symbol}] 完成，共 {len(merged)} 只。")
    return merged[["index_symbol", "akshare_code", "code", "name", "in_date", "fetched_at"]]
