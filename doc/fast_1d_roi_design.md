# 1日优先 · ROI优先 · 快速筛选预测方案

## 目标
- **投入产出比（ROI）优先**：以每次投入 100 为基准，追求次日利润最大化。
- **胜率其次**：买 10 次对的越多越好，但不能牺牲 ROI。
- **天数越短越好**：优先 1 天预测，不行再切 5 天。
- **流程必须快**：不做排列组合，不跑不完。

## 非目标
- 不追求“只有短周期才行”，如果 5 天更稳、更赚钱，就用 5 天。
- 不做全量网格搜索（主要原因：太慢，不符合快速出结果的要求）。
- 不排斥传统误差指标（RMSE/MAE），但它们只作为辅助参考。

## 核心评分函数（ROI优先）
设每笔投入 100：
- **ROI**：平均盈利 / 100
- **WinRate**：正收益次数 / 总次数
- **Horizon惩罚**：天数越长扣分

**Score = 0.7 * ROI + 0.2 * WinRate - 0.1 * (H / 5)**

## 评估指标（兼顾传统指标）
- ROI（主目标）
- WinRate（稳定性）
- AvgReturn（平均收益）
- RMSE/MAE（传统误差指标，仅作辅助）
- 可选：Profit Factor（盈利 / 亏损）

## 固定参数（避免组合爆炸）
- **Horizon**：先 1 天，若 ROI/WinRate 明显不佳，再切 5 天（优先“更赚钱、更稳定”的天数）
- **Train days**：60
- **Test days**：20
- **Feature set**：basic

## 代码级流程（可直接执行）

### 1）极速筛组选“最可能赚钱的组”
脚本：
```
bash scripts/run_fast_track_discovery.sh
```
说明：
- 只跑 3 个代表性组
- 每组只抽 5 支股票
- 只跑 1 天
- 对比 basic / full

结果：
- 看日志里的 `Overall Hit Rate` 和 `RMSE`
- 结合 ROI 目标，选 1~2 个“最可能赚钱的组”

### 2）在最优组里做单股快速评估
脚本：
```
bash scripts/run_single_stock_eval.sh <symbol> 60 1 basic 20
```
示例：
```
bash scripts/run_single_stock_eval.sh 002594 60 1 basic 20
```

输出：
- `data/tasks/eval_single_<symbol>_*/logs/eval.log`

### 3）若 1 天 ROI 过弱或稳定性差，切 5 天
脚本：
```
bash scripts/run_single_stock_eval.sh <symbol> 60 5 basic 20
```

### 4）输出 Top-K（ROI优先排序）
建议输出字段：
- symbol
- ROI
- WinRate
- Score
- AvgReturn

## 快速决策策略（避免跑不完）
- 只做 1 天，筛不出再切 5 天。
- 特征先固定 `basic`，只有在小名单内再试 `full`。
- 每组只抽 5~10 只票。
- Top-K 直接进入实盘观察。

## 风险与应对
- **1天噪声大**：用 ROI+WinRate 双指标稳住。
- **特征过多过拟合**：先只用 `basic`。
- **样本小误判**：只在“高流动性 + 中高波动”股票上筛。

## 下一步（如果要自动化）
- 写一个小脚本解析 `eval.log` 计算 ROI/WinRate/Score。
- 自动输出 Top-K CSV，直接用于实盘筛选。
