---
name: single-stock-prediction
description: 单股实效预测技能。指导如何在评估出最优参数后，生成每日预测结果，并管理预测产物。
---

# 单股实效预测 (Single Stock Prediction)

本技能指导用户在完成 `single-stock-research` 评估后，如何将最优参数转化为每日的实际预测操作。

## 适用场景

- 已通过回测确定了某只股票的最优 `Context Length` 和 `Feature Set`。
- 需要生成针对明天的价格预测。
- 需要自动化记录每日预测结果以供复盘。

## 执行流程

### 1. 准备最优适配器 (Adapter)

确保你已经运行过 `run_single_stock_eval.sh` 并获得了产物：
- 适配器权重：`data/tasks/eval_single_<symbol>_<ts>/adapters/adapter.pth`
- 建议参数：见 `eval.log` 末尾的推荐命令。

### 2. 生成预测

使用 `run_single_stock_predict.sh` 脚本。该脚本已默认切换为 **AkShare** 实时数据源，以确保在收盘后能立即获取最新的历史数据进行预测（避免 DuckDB 等本地数据库更新延迟）。

**示例命令**：
```bash
# 语法: bash scripts/run_single_stock_predict.sh <symbol> <adapter_path> [horizon] [context_len]
bash scripts/run_single_stock_predict.sh 002074 data/tasks/eval_single_002074_latest/adapters/adapter.pth 1 60
```

### 3. 结果解读

预测产物将保存在 `data/tasks/predict_single_<symbol>_<ts>/predictions/` 目录下。
- **`forecast_results.csv`**: 包含 T+1 的预测价格。
- **`forecast_plot.png`** (如果启用): 直观显示预测点与历史曲线的关系。

---

## 📈 实战笔记：如何提高预测实效性

- **定期重训**：单股的波动特性会随市场情绪变化。建议每 5-10 个交易日重新执行一次 `run_single_stock_eval.sh` 以更新适配器权重。
- **多模型验证**：如果 1天预测 (Horizon=1) 和 5天预测 (Horizon=5) 的方向一致，则预测的可信度更高。
- **配合分组预测**：参考 `run_auto_top_picks.sh`，观察个股所属的行业指数表现，如果行业指数也处于上升周期，则个股预测的成功率会大幅提升。
