import os
import sys
import torch
from pathlib import Path
from typing import Any, List, Optional

import numpy as np


def _find_project_root() -> Path:
    """寻找当前项目的根目录（即包含 local_timesfm_model 的目录）。"""
    for parent in [Path(__file__).resolve()] + list(Path(__file__).resolve().parents):
        if (parent / "local_timesfm_model").exists() or (parent / "pyproject.toml").exists():
            return parent
    return Path(__file__).resolve().parents[2]


PROJECT_ROOT = _find_project_root()
_TIMESFM_SRC = PROJECT_ROOT / "timesfm-master" / "src"

if not (_TIMESFM_SRC / "timesfm").exists():
    raise ImportError(f"未找到本地 TimesFM 源码目录: {_TIMESFM_SRC}")

if str(_TIMESFM_SRC) not in sys.path:
    sys.path.insert(0, str(_TIMESFM_SRC))

_timesfm_import_error: Exception | None = None

try:
    from timesfm import TimesFM_2p5_200M_torch, ForecastConfig
except Exception as exc:
    TimesFM_2p5_200M_torch = None
    ForecastConfig = None
    _timesfm_import_error = exc

from .finetuning import LinearAdapter, load_adapter
from .features import FeatureExtractor


def 默认模型目录() -> str:
    env_model_path = os.environ.get("TIMESFM_MODEL_PATH")
    if env_model_path:
        return str(Path(env_model_path).expanduser().resolve())
    return str(PROJECT_ROOT / "local_timesfm_model")


def 加载模型(model_dir: str | None) -> Any:
    if TimesFM_2p5_200M_torch is None:
        raise ImportError("未发现可用 TimesFM 运行时，请确认本地 timesfm-master/src 可用。") from _timesfm_import_error
    实际目录 = model_dir or 默认模型目录()
    model = TimesFM_2p5_200M_torch.from_pretrained(实际目录, torch_compile=False)
    model.compile(
        ForecastConfig(
            max_context=1024,
            max_horizon=256,
            normalize_inputs=True,
            use_continuous_quantile_head=True,
            force_flip_invariance=True,
            infer_is_positive=False,
            fix_quantile_crossing=True,
        )
    )
    return model

def 运行预测(
    model: Any,
    序列: np.ndarray,
    context_length: int,
    horizon: int,
) -> tuple[np.ndarray, np.ndarray]:
    输入 = 序列[-context_length:] if 序列.size > context_length else 序列
    点预测, 分位数预测 = model.forecast(horizon=horizon, inputs=[输入.astype(np.float32)])
    return 点预测[0], 分位数预测[0]

class AdvancedStockModel:
    """
    高级股票预测模型包装类。
    支持：
    1. TimesFM 基础预测。
    2. 线性适配器 (Linear Adapter) 残差修正。
    """
    def __init__(
        self,
        base_model: Optional[Any] = None,
        adapter: Optional[LinearAdapter] = None
    ):
        self.base_model = base_model
        self.adapter = adapter
        self.device = torch.device("cpu")
        
        if self.base_model and hasattr(self.base_model, 'model'):
            self.base_model.model.to(self.device)

    def forecast(
        self, 
        inputs: List[np.ndarray], 
        horizon: int,
        ohlcv_inputs: Optional[List[np.ndarray]] = None,
        **kwargs
    ):
        """
        进行预测。如果加载了适配器，则应用残差修正。
        """
        if not self.base_model:
            raise RuntimeError("基础 TimesFM 模型未加载。")

        # 1. 基础预测
        pts, qts = self.base_model.forecast(horizon=horizon, inputs=inputs, **kwargs)
        
        # 2. 如果没有适配器，直接返回基础预测
        if not self.adapter:
            return pts, qts

        # 3. 应用适配器修正 (仅针对点预测/中位数)
        adjusted_pts = pts.copy()
        adjusted_qts = qts.copy()
        expected_dim = len(self.adapter.weights.coef) - 1  # coef 含截距项
        for i, context in enumerate(inputs):
            base_val = pts[i, 0]
            ohlcv_context = ohlcv_inputs[i] if ohlcv_inputs and len(ohlcv_inputs) > i else None
            # 从适配器中读取训练时使用的特征集
            feature_names = self.adapter.weights.feature_names
            features = FeatureExtractor.compute(context, base_val, ohlcv_context=ohlcv_context, feature_names=feature_names)

            if features.shape[0] != expected_dim:
                raise ValueError(
                    f"适配器特征维度不匹配: 期望 {expected_dim}, 实际 {features.shape[0]}。"
                    f" feature_names={feature_names}"
                )

            # 使用适配器修正
            residual = self.adapter.apply(features.reshape(1, -1))[0]
            adjusted_pts[i, 0] += residual
            adjusted_qts[i, :, :] += residual

        return adjusted_pts, adjusted_qts

def load_advanced_model(
    model_dir: Optional[str] = None,
    adapter_path: Optional[str] = None,
) -> AdvancedStockModel:
    """加载高级模型。"""
    if TimesFM_2p5_200M_torch is None:
        raise ImportError("未发现可用 TimesFM 运行时，请确认本地 timesfm-master/src 可用。") from _timesfm_import_error

    base_model = 加载模型(model_dir)
    
    adapter = None
    if adapter_path and os.path.exists(adapter_path):
        print(f"正在加载微调适配器: {adapter_path}")
        weights = load_adapter(adapter_path)
        adapter = LinearAdapter(weights)
    
    return AdvancedStockModel(base_model, adapter)
