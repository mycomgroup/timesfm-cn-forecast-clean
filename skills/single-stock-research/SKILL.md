---
name: single-stock-research
description: 单股深度研究技能。指导如何针对特定股票进行多因子对比实验、参数排列组合搜索，以及回测结果的系统化评估。
---

# 单股深度研究 (Single Stock Deep Research)

本技能旨在指导研究员如何针对特定的 A 股标的，利用 TimesFM 寻找最契合该股票波动特性的因子组合与模型参数。

## 适用场景

- 发现某只股票在基础模型下预测不准，需要通过微调 (Finetuning) 提升精度。
- 需要测试新开发的技术指标 (Factor) 对特定标的是否有效。
- 探索针对不同预测步长 (Horizon) 的最优历史上下文长度 (Context Length)。

## 核心流程：参数实验三部曲

### 1. 确定实验场景表 (Permutation Table)

在开始实验前，应根据研究目的设定参数矩阵。常见的维度包括：

- **特征集 (FEATURE_SET)**: `basic`, `technical`, `structural`, `full`。
- **训练长度 (TRAIN_DAYS)**: `60` (短线), `500` (中长线)。
- **预测步长 (HORIZON)**: `1` (T+1), `5` (一周趋势)。

### 2. 执行批量评估

利用重构后的 `run_single_stock_eval.sh` 脚本，可以自动遍历多个 `CONTEXT_LEN` (30, 60, 90, 128, 256, 512)。

**示例：测试短线全特征模式**
```bash
FEATURE_SET=full bash scripts/run_single_stock_eval.sh 002074 60 1
```

### 3. 指标对齐与偏差分析

观察 `eval.log` 中的回测汇总表，重点关注：
- **RMSE**: 均方根误差。如果 RMSE 随 ContextLen 增加而剧增，说明模型在该股票上过拟合严重。
- **HitRate**: 方向准确率。在金融预测中，55% 以上的 HitRate 通常被认为具有较高的实战价值。

---

## 💡 进阶：如何增加新因子

1.  **修改代码**：在 `src/timesfm_cn_forecast/features.py` 的 `generate_features_dict` 中加入计算逻辑。
2.  **更新特征集**：在 `FEATURE_SETS` 字典中将新因子名称加入 `full` 或自定义组。
3.  **对比实验**：仅改变 `FEATURE_SET` 参数运行两次评估，对比指标变化。

## 4. 实战案例：国轩高科 (002074) 实验结论

经过对 002074 的 48 组参数排列组合测试，得出以下核心结论，可作为后续研究的基准：

### 场景 A：短线波弈 (60天训练 $\rightarrow$ 1天预测)
- **最优配置**：`FEATURE_SET=full`, `CONTEXT_LEN=60`。
- **表现**：HitRate ~58%, RMSE ~0.48。
- **经验**：在小样本（60天）训练下，上下文长度超过 90 会导致 RMSE 急剧恶化。

### 场景 B：趋势跟随 (500天训练 $\rightarrow$ 5天预测)
- **最优配置**：`FEATURE_SET=full`, `CONTEXT_LEN=90`。
- **表现**：HitRate ~52%, RMSE ~0.68。
- **经验**：中长期预测需要更长的历史窗口（90-128天）来平滑噪音。

## 5. 从研究到预测的转化

当研究确定最优参数后，应立即转入实效预测环节：

1.  **保存适配器**：记录最优运行产生的 `adapter.pth` 路径。
2.  **执行预测命令**：
    ```bash
    # 使用 A 场景最优参数
    bash scripts/run_single_stock_predict.sh 002074 <adapter_path> 1 60
    
    # 使用 B 场景最优参数
    bash scripts/run_single_stock_predict.sh 002074 <adapter_path> 5 90
    ```
3.  **每日复盘**：对比预测值与次日实际开收盘价，动态调整研究方向。
## 6. 可视化优化（Visualization Best Practice）

在处理上市时间较长的股票时，绘图可能会因为历史数据点过多而导致最近的预测点被挤压。本项目已在 `pipeline.py` 中内置了自动切片逻辑：

- **动态缩窗**：绘图时仅展示最近 `context_length + horizon + 30` 个交易日的数据。
- **目的**：确保 X 轴范围高度聚焦，在 1天 (T+1) 或 5天 (T+5) 预测场景下，能清晰观测到预测曲线的微小斜率変化。
- **自定义建议**：如果需要查看更长期的周期性特征，可以在 `providers` 中使用 `--start` 参数手动控制加载的数据总量。
