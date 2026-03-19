import numpy as np
from typing import Dict, List, Optional

FEATURE_SETS = {
    "basic": [
        "base_pred", "last_price", "mean_price", "pct_change", "volatility", "momentum_5", "momentum_10"
    ],
    "technical": [
        "base_pred", "last_price", "mean_price", "pct_change", "volatility",
        "macd", "macd_signal", "macd_hist", "rsi", "boll_upper", "boll_lower",
        "momentum_5", "momentum_10", "trend_slope", "price_zscore", "vol_ratio",
        "open", "high", "low", "volume"
    ],
    "structural": [
        "base_pred", "last_price", "mean_price", "pct_change", "volatility",
        "body_direction", "body_ratio", "upper_ratio", "lower_ratio",
        "close_position", "open_position", "range_ratio", "gap",
        "body_change", "volume", "momentum_5", "trend_slope", "vol_ratio"
    ],
    "full": [
        "base_pred", "last_price", "mean_price", "pct_change", "volatility",
        "macd", "macd_signal", "macd_hist", "rsi", "boll_upper", "boll_lower",
        "open", "high", "low", "close", "volume",
        "body_direction", "body_ratio", "upper_ratio", "lower_ratio",
        "close_position", "open_position", "range_ratio", "gap",
        "body_change", "true_range", "close_norm",
        "momentum_5", "momentum_10", "trend_slope", "price_zscore", "vol_ratio",
    ],
}


def get_feature_names(mode: str) -> List[str]:
    if mode not in FEATURE_SETS:
        raise ValueError(f"未知的特征组合: {mode}。可选: {list(FEATURE_SETS.keys())}")
    return FEATURE_SETS[mode]


def _safe_pct(a: float, b: float) -> float:
    return float((a - b) / b) if abs(b) > 1e-8 else 0.0


def generate_features_dict(
    context: np.ndarray,
    base_pred: float,
    ohlcv_context: Optional[np.ndarray],
    mode: str = "technical",
) -> Dict[str, float]:
    if len(context) == 0:
        return {k: 0.0 for k in get_feature_names(mode)}

    feats: Dict[str, float] = {}
    last_price = float(context[-1])
    first_price = float(context[0])
    mean_price = float(np.mean(context))
    std_price = float(np.std(context))
    inv_last = 1.0 / last_price if abs(last_price) > 1e-8 else 0.0

    feats["base_pred"] = float(_safe_pct(float(base_pred), last_price))
    feats["last_price"] = 0.0
    feats["mean_price"] = float(_safe_pct(mean_price, last_price))
    feats["pct_change"] = float(_safe_pct(last_price, first_price))
    feats["volatility"] = float(std_price * inv_last)
    feats["close_norm"] = 1.0

    if len(context) >= 6:
        feats["momentum_5"] = float(_safe_pct(last_price, float(context[-6])))
    else:
        feats["momentum_5"] = 0.0
    if len(context) >= 11:
        feats["momentum_10"] = float(_safe_pct(last_price, float(context[-11])))
    else:
        feats["momentum_10"] = 0.0

    if len(context) >= 20 and std_price > 1e-8:
        feats["price_zscore"] = float((last_price - mean_price) / std_price)
    else:
        feats["price_zscore"] = 0.0

    if len(context) >= 10:
        x = np.arange(len(context), dtype=np.float32)
        slope = np.polyfit(x, context.astype(np.float32), deg=1)[0]
        feats["trend_slope"] = float(slope * inv_last)
    else:
        feats["trend_slope"] = 0.0

    def ema(data: np.ndarray, window: int) -> np.ndarray:
        alpha = 2.0 / (window + 1)
        res = np.zeros_like(data, dtype=np.float32)
        res[0] = data[0]
        for i in range(1, len(data)):
            res[i] = alpha * data[i] + (1 - alpha) * res[i - 1]
        return res

    if len(context) >= 30:
        ema12, ema26 = ema(context, 12), ema(context, 26)
        macd_line = ema12 - ema26
        signal_line = ema(macd_line, 9)
        macd_hist = macd_line - signal_line

        feats["macd"] = float(macd_line[-1] * inv_last)
        feats["macd_signal"] = float(signal_line[-1] * inv_last)
        feats["macd_hist"] = float(macd_hist[-1] * inv_last)

        delta = np.diff(context)
        gain, loss = np.where(delta > 0, delta, 0), np.where(delta < 0, -delta, 0)
        avg_gain = np.mean(gain[-14:]) if len(gain) >= 14 else 0.0
        avg_loss = np.mean(loss[-14:]) if len(loss) >= 14 else 0.0
        feats["rsi"] = 100.0 if avg_loss == 0 else float(100.0 - (100.0 / (1.0 + avg_gain / avg_loss)))

        ma20, std20 = np.mean(context[-20:]), np.std(context[-20:])
        feats["boll_upper"] = float((ma20 + 2 * std20 - last_price) * inv_last)
        feats["boll_lower"] = float((ma20 - 2 * std20 - last_price) * inv_last)
    else:
        for k in ["macd", "macd_signal", "macd_hist", "rsi", "boll_upper", "boll_lower"]:
            feats[k] = 0.0

    if ohlcv_context is not None and len(ohlcv_context) >= 2:
        op, hi, lo, cl, vol = [float(x) for x in ohlcv_context[-1]]
        p_op, p_hi, p_lo, p_cl, p_vol = [float(x) for x in ohlcv_context[-2]]

        feats.update(
            {
                "open": float((op - last_price) * inv_last),
                "high": float((hi - last_price) * inv_last),
                "low": float((lo - last_price) * inv_last),
                "close": float((cl - last_price) * inv_last),
                "volume": float(vol / p_vol) if p_vol > 0 else 1.0,
            }
        )
        feats["close_norm"] = float(cl / p_cl) if abs(p_cl) > 1e-8 else 1.0

        if len(ohlcv_context) >= 20:
            vol_hist = ohlcv_context[-20:, 4]
            mean_vol = float(np.mean(vol_hist))
            feats["vol_ratio"] = float(vol / mean_vol) if mean_vol > 1e-8 else 1.0
        else:
            feats["vol_ratio"] = float(vol / p_vol) if p_vol > 0 else 1.0

        body = abs(cl - op)
        rng_raw = hi - lo
        up_sh = hi - max(op, cl)
        lo_sh = min(op, cl) - lo
        rng = rng_raw if rng_raw > 1e-8 else 1e-8

        feats["body_direction"] = float(np.sign(cl - op))
        feats["body_ratio"] = float(np.clip(body / rng, 0, 1))
        feats["upper_ratio"] = float(np.clip(up_sh / rng, 0, 1))
        feats["lower_ratio"] = float(np.clip(lo_sh / rng, 0, 1))
        feats["close_position"] = float(np.clip((cl - lo) / rng, 0, 1))
        feats["open_position"] = float(np.clip((op - lo) / rng, 0, 1))
        feats["gap"] = 1.0 if op > p_hi else (-1.0 if op < p_lo else 0.0)
        feats["range_ratio"] = float(rng_raw / p_cl) if abs(p_cl) > 1e-8 else 0.0
        p_body = abs(p_cl - p_op)
        feats["body_change"] = float(np.clip(body / max(p_body, 1e-8), 0, 5))
        feats["true_range"] = float(max(hi - lo, abs(hi - p_cl), abs(lo - p_cl)) * inv_last)
    else:
        for k in [
            "open", "high", "low", "close", "volume", "body_direction", "body_ratio", "upper_ratio",
            "lower_ratio", "close_position", "open_position", "range_ratio", "gap", "body_change", "true_range",
        ]:
            feats[k] = 0.0
        feats["vol_ratio"] = 1.0

    keys = get_feature_names(mode)
    out_dict = {}
    for k in keys:
        val = feats.get(k, 0.0)
        out_dict[k] = float(np.clip(val, -1e6, 1e6))
    return out_dict


class FeatureExtractor:
    """提供给已有代码获取 Numpy 数组的接口包装。"""

    @staticmethod
    def compute(
        context: np.ndarray,
        base_pred: float,
        ohlcv_context: Optional[np.ndarray],
        feature_names: List[str],
    ) -> np.ndarray:
        full_dict = generate_features_dict(context, base_pred, ohlcv_context, mode="full")
        out = [full_dict.get(name, 0.0) for name in feature_names]
        return np.array(out, dtype=np.float32)
