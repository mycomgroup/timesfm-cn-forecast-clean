from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import torch
from sklearn.linear_model import HuberRegressor, Ridge
from sklearn.preprocessing import StandardScaler

from .features import FeatureExtractor, get_feature_names


@dataclass
class AdapterWeights:
    coef: np.ndarray
    mean: np.ndarray
    scale: np.ndarray
    feature_names: List[str]
    context_len: int
    horizon_len: int
    stock_code: Optional[str] = None


class LinearAdapter:
    def __init__(self, weights: AdapterWeights):
        self.weights = weights

    def apply(self, features: np.ndarray) -> np.ndarray:
        scaled_features = (features - self.weights.mean) / self.weights.scale
        ones = np.ones((scaled_features.shape[0], 1), dtype=np.float32)
        x_aug = np.concatenate([scaled_features, ones], axis=1)
        residuals = x_aug @ self.weights.coef
        return residuals


def train_linear_adapter(
    train_X: np.ndarray,
    train_y: np.ndarray,
    train_base: np.ndarray,
    context_len: int,
    horizon_len: int,
    feature_names: List[str],
    stock_code: Optional[str] = None,
    model_type: str = "ridge",
    ridge_alpha: float = 0.1,
    huber_epsilon: float = 1.35,
    sample_weights: Optional[np.ndarray] = None,
) -> AdapterWeights:
    """训练线性残差适配器。"""
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(train_X)
    residuals = train_y - train_base

    x_scaled = np.nan_to_num(x_scaled, nan=0.0, posinf=0.0, neginf=0.0)
    residuals = np.nan_to_num(residuals, nan=0.0, posinf=0.0, neginf=0.0)

    if sample_weights is not None:
        sample_weights = np.asarray(sample_weights, dtype=np.float32)

    model_type = model_type.lower()
    if model_type == "lstsq":
        x_aug = np.concatenate([x_scaled, np.ones((x_scaled.shape[0], 1), dtype=np.float32)], axis=1)
        coef, residuals_sum, rank, _ = np.linalg.lstsq(x_aug, residuals, rcond=0.01)
        y_pred_res = x_aug @ coef
        print(f"  [Adapter:lstsq] rank={rank}, res_sum={float(np.sum(residuals_sum)) if residuals_sum.size else 0.0:.4e}")
    elif model_type == "ridge":
        model = Ridge(alpha=ridge_alpha, fit_intercept=True, random_state=42)
        model.fit(x_scaled, residuals, sample_weight=sample_weights)
        coef = np.concatenate([model.coef_.astype(np.float32), np.array([model.intercept_], dtype=np.float32)])
        y_pred_res = model.predict(x_scaled)
    elif model_type == "huber":
        model = HuberRegressor(epsilon=huber_epsilon, alpha=ridge_alpha, fit_intercept=True)
        model.fit(x_scaled, residuals, sample_weight=sample_weights)
        coef = np.concatenate([model.coef_.astype(np.float32), np.array([model.intercept_], dtype=np.float32)])
        y_pred_res = model.predict(x_scaled)
    else:
        raise ValueError("model_type must be one of: lstsq, ridge, huber")

    ss_res = float(np.sum((residuals - y_pred_res) ** 2))
    ss_tot = float(np.sum((residuals - np.mean(residuals)) ** 2))
    r2 = 1 - (ss_res / ss_tot) if ss_tot > 1e-8 else 0.0
    print(f"  [Adapter:{model_type}] Training R2 Score: {r2:.4f}")

    return AdapterWeights(
        coef=coef.astype(np.float32),
        mean=scaler.mean_.astype(np.float32),
        scale=scaler.scale_.astype(np.float32),
        feature_names=feature_names,
        context_len=context_len,
        horizon_len=horizon_len,
        stock_code=stock_code,
    )


def save_adapter(weights: AdapterWeights, path: str):
    data = {
        "adapter_coef": weights.coef,
        "scaler_mean": weights.mean,
        "scaler_scale": weights.scale,
        "feature_names": weights.feature_names,
        "context_len": weights.context_len,
        "horizon_len": weights.horizon_len,
        "stock_code": weights.stock_code,
    }
    torch.save(data, path)


def load_adapter(path: str) -> AdapterWeights:
    data = torch.load(path, map_location="cpu", weights_only=False)
    return AdapterWeights(
        coef=data["adapter_coef"],
        mean=data["scaler_mean"],
        scale=data["scaler_scale"],
        feature_names=data["feature_names"],
        context_len=data["context_len"],
        horizon_len=data["horizon_len"],
        stock_code=data.get("stock_code"),
    )


def main() -> None:
    import argparse
    from pathlib import Path

    import pandas as pd

    parser = argparse.ArgumentParser(description="训练线性残差适配器。")
    parser.add_argument("--stock-code", type=str, required=True, help="股票代码")
    parser.add_argument("--data-path", type=str, required=True, help="训练数据 (history.csv) 的路径")
    parser.add_argument("--output-path", type=str, required=True, help="适配器权重 (.pth) 的保存路径")
    parser.add_argument("--context-len", type=int, default=60, help="上下文长度")
    parser.add_argument("--horizon-len", type=int, default=1, help="预测步长")
    parser.add_argument("--feature-set", type=str, default="technical", help="使用的特征组合名称")
    parser.add_argument("--train-days", type=int, default=None, help="仅使用最近 N 天的数据进行训练")
    parser.add_argument("--train-end", type=str, default=None, help="训练截止日 YYYY-MM-DD")
    parser.add_argument("--model-type", type=str, default="ridge", choices=["lstsq", "ridge", "huber"])
    parser.add_argument("--ridge-alpha", type=float, default=0.1)
    parser.add_argument("--huber-epsilon", type=float, default=1.35)

    args = parser.parse_args()

    stock_code = args.stock_code
    data_path = Path(args.data_path)
    save_path = Path(args.output_path)

    print(f"开始为股票 {stock_code} 训练线性适配器...")
    save_path.parent.mkdir(parents=True, exist_ok=True)

    if not data_path.exists():
        raise FileNotFoundError(f"数据文件不存在: {data_path}")

    df = pd.read_csv(data_path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date")

    if args.train_end and "date" in df.columns:
        train_end = pd.Timestamp(args.train_end)
        df = df[df["date"] <= train_end]

    df = df.ffill().bfill()

    if args.train_days:
        df = df.tail(args.train_days + args.context_len + args.horizon_len)

    prices = df["value"].to_numpy(dtype=np.float32)
    ohlcv_cols = ["open", "high", "low", "close", "volume"]
    ohlcv = df[ohlcv_cols].to_numpy(dtype=np.float32) if all(c in df.columns for c in ohlcv_cols) else None

    feature_names = get_feature_names(args.feature_set)
    n_samples = len(prices) - args.context_len - args.horizon_len + 1
    if n_samples <= 0:
        raise ValueError("数据量不足以进行微调训练")

    samples = []
    targets = []
    base_preds = []

    for i in range(n_samples):
        context = prices[i : i + args.context_len]
        target = float(prices[i + args.context_len + args.horizon_len - 1])
        base_pred = float(context[-1])
        ohlcv_context = ohlcv[i : i + args.context_len] if ohlcv is not None else None
        feats = FeatureExtractor.compute(context, base_pred, ohlcv_context=ohlcv_context, feature_names=feature_names)
        samples.append(feats)
        targets.append(target)
        base_preds.append(base_pred)

    train_x = np.array(samples, dtype=np.float32)
    train_y = np.array(targets, dtype=np.float32)
    train_base = np.array(base_preds, dtype=np.float32)

    weights = train_linear_adapter(
        train_X=train_x,
        train_y=train_y,
        train_base=train_base,
        context_len=args.context_len,
        horizon_len=args.horizon_len,
        feature_names=feature_names,
        stock_code=stock_code,
        model_type=args.model_type,
        ridge_alpha=args.ridge_alpha,
        huber_epsilon=args.huber_epsilon,
    )

    save_adapter(weights, str(save_path))
    print(f"适配器权重已保存至: {save_path}")


if __name__ == "__main__":
    main()
