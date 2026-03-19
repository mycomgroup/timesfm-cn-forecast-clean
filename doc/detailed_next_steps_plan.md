# 后续详细执行方案

更新时间：2026-03-19

这份文档不是再讲大方向，而是直接回答下面几个问题：

- 现在代码第一步该怎么改
- 哪些数据还缺
- 哪些结论必须重新验证
- 后续按什么顺序推进最省时间

目标只有一个：

在尽量少浪费算力的前提下，找到“最近滚动最稳、次日或 5 日最有交易价值”的股票和股票组，并能稳定输出候选列表。

---

## 总原则

1. 先修评估，再调参数。
2. 先 baseline 扫组，再做 patch 精调。
3. 先看最近滚动，再看全历史均值。
4. 先找组，再找组内前三。
5. 误差指标和交易指标分开记，不要混。

---

## 第 1 步：先把评估口径修真

### 1.1 要解决的问题

当前三套仓库共同存在的最大问题，是训练集和测试集疑似重叠。

如果这一步不先修：
- `patch` 是否有效，没法判断
- `single vs group` 是否真的更优，没法判断
- `huber vs ridge` 的比较也会失真
- 很多 `rmse=0`、高命中率、高 ROI 可能只是“看过答案”

所以第 1 步不是继续跑实验，而是先把评估切分改成真正的 walk-forward。

### 1.2 需要改哪些代码

#### A. `src/timesfm_cn_forecast/backtest.py`

目标：
- 支持显式传入训练结束日
- 支持测试起止区间
- 支持 recent rolling metrics 输出

建议改动：
- 给 `run_backtest()` 新增参数：
  - `train_end_date: Optional[str] = None`
  - `test_start_date: Optional[str] = None`
  - `test_end_date: Optional[str] = None`
  - `rolling_windows: Optional[List[int]] = None`
- 回测时，严格保证：
  - 每次预测的上下文只来自当前预测日前的数据
  - adapter 的训练样本只来自 `train_end_date` 及之前

需要新增输出字段：
- `AvgRet`
- `CumRet`
- `MaxDrawdown`
- `ProfitFactor`
- `Recent20_HitRate`
- `Recent20_AvgRet`
- `Recent40_HitRate`
- `Recent40_AvgRet`
- `Recent60_HitRate`
- `Recent60_AvgRet`

建议新增函数：
- `calculate_trading_metrics()`
- `summarize_recent_windows()`

#### B. `src/timesfm_cn_forecast/run_group_eval.py`

目标：
- 训练样本和测试样本彻底分开
- 组训练和回测使用同一套真实切分逻辑

建议改动：
- 增加训练截止参数：
  - `--train-end`
  - `--test-start`
  - `--test-end`
- `_build_training_samples()` 中只取 `train_end` 以前的数据
- `run_backtest()` 调用时把测试窗口显式传进去

新增输出字段到 `results_*.csv`：
- `avg_ret`
- `cum_ret`
- `profit_factor`
- `max_drawdown`
- `recent20_avg_ret`
- `recent20_hitrate`
- `recent40_avg_ret`
- `recent40_hitrate`
- `recent60_avg_ret`
- `recent60_hitrate`

#### C. `scripts/run_single_stock_eval.sh`

目标：
- 单股评估也使用统一切分

建议加参数：
- `TRAIN_END`
- `TEST_START`
- `TEST_END`

脚本逻辑改成：
1. 拉取全量历史
2. 训练时只用 `TRAIN_END` 前数据
3. 回测只用 `TEST_START` 到 `TEST_END`

#### D. `timesfm_autoresearch/experiment.py`

虽然现在不在这个仓库里改，但后面必须同步做同样的切分修复。

需要改的点：
- `_train_from_single()`
- `_train_from_group()`
- `run_experiment()`

逻辑同上：
- 训练样本只到训练截止日
- 测试永远从下一个时间段开始

### 1.3 需要补什么数据

第 1 步主要不是补新行情，而是补“评估元数据”：

- 每次实验的训练截止日
- 测试起止日
- rolling window 长度
- 是否使用 patch
- 是否 `single/group`
- 是否 `huber/ridge`
- 是否 `alpha target`
- 是否 `momentum on/off`

建议做法：
- 新增统一实验记录表，至少写成 CSV
- 或者直接写入 `data/tasks/<task>/meta.json`

### 1.4 第 1 步要验证什么结论

第 1 步修完后，先不要急着继续扩参数，只验证 4 件事：

1. `group` 在无泄漏条件下，是否仍优于 `single`
2. `patch` 在无泄漏条件下，是否仍优于 baseline
3. `1天` 和 `5天` 的 recent rolling 收益谁更稳定
4. “最近 20/40/60 天最强” 和 “全历史均值最强” 是否明显不同

### 1.5 第 1 步交付物

必须产出：
- `group_recent_summary.csv`
- `stock_recent_summary.csv`
- 每个结果文件都包含 recent rolling 指标
- 一份结论表：`validation_after_split_fix.md`

---

## 第 2 步：先用 baseline 扫组，建立候选池

### 2.1 要解决的问题

现在不是缺参数，而是缺“靠谱的候选组”。

你的最终目标不是全市场预测所有股票，而是：
- 找一批有规律的组
- 每组找到最稳的前几支

所以第 2 步的核心是做“组级普查”。

### 2.2 要补哪些组

建议先统一成 4 大类：

#### A. 行业组
- 直接复用当前 `index_market.duckdb` 中已有行业组

#### B. 概念组
- 直接复用当前已有概念组

#### C. 模型推断的组
- 这里主要指共振相关组
- 以某只种子股为起点，筛 50 支高相关股票

#### D. 波动率组
- 按最近 60 天或 120 天波动率，把股票分层
- 比如：
  - `vol_low`
  - `vol_mid`
  - `vol_high`
  - `vol_spike`

### 2.3 需要改哪些代码

#### A. `src/timesfm_cn_forecast/universe/fetcher.py`

目标：
- 把“动态衍生分组”正规化

建议新增支持：
- 读取本地 JSON 或 CSV 生成 group
- 支持：
  - `resonance_*`
  - `vol_*`
  - `seed_*`

建议：
- 不要把所有逻辑都塞进 `INDEX_MAP`
- 新增一个外部文件目录：
  - `data/group_definitions/`

里面可以放：
- `resonance_groups.json`
- `volatility_groups.json`
- `seed_groups.json`

#### B. 新增 `src/timesfm_cn_forecast/build_dynamic_groups.py`

这个脚本负责生成动态组定义。

需要支持三种生成方式：

1. `resonance`
   - 输入：种子股票列表
   - 输出：每只种子股对应 50 支高相关股票

2. `volatility`
   - 输入：近 60/120 天行情
   - 输出：按波动率分桶的组

3. `seed expansion`
   - 输入：人工指定种子股
   - 输出：前 50 个相似股票

#### C. `scripts/run_baseline_all_groups.sh`

目标：
- 增加“只看最近窗口”的能力
- 输出统一 summary

建议加参数：
- `RECENT_WINDOWS=20,40,60`
- `SAMPLE_SIZE=10`
- `MAX_GROUPS=30`

#### D. 新增 `scripts/run_recent_group_baseline.sh`

建议新脚本专门做你现在最想要的那种“快速普查”：

功能：
- 只跑最近 20/40/60 天
- 每组随机 10 支
- 先不加 patch
- 跑 30 个重点组

输出：
- `data/tasks/recent_group_baseline_<ts>/group_recent_summary.csv`

### 2.4 第 2 步如何组织实验

#### 第一轮：快筛

配置固定：
- `sample_size=10`
- `context_len=30 或 60`
- `horizon=1`
- `test_days=60`
- `patch=off`

目的：
- 快速找到 10 到 20 个“最近状态靠谱”的组

#### 第二轮：扩容验证

对第一轮中最好的组：
- 每组从 10 支扩大到 30 支
- 再跑 recent 20/40/60
- 继续保持 patch=off

目的：
- 验证是不是抽样误差

### 2.5 第 2 步要验证什么结论

必须重点验证：

1. 哪类组最稳定：
- 行业
- 概念
- 共振
- 波动率

2. 组内是不是存在稳定的“前三支”

3. 哪些组最近 20 天强，但最近 60 天已经弱化

4. 哪些组适合 1 天，哪些组适合 5 天

### 2.6 第 2 步交付物

必须产出：
- `group_recent_summary.csv`
- `group_top3_candidates.csv`
- `group_candidate_pool.csv`

其中 `group_top3_candidates.csv` 至少包含：
- `group`
- `symbol`
- `recent20_avg_ret`
- `recent20_hitrate`
- `recent40_avg_ret`
- `recent60_avg_ret`
- `stability_score`

---

## 第 3 步：只对重点组做精调和交易输出

### 3.1 要解决的问题

第 3 步才进入真正的模型精调。

注意顺序：
- 不是全市场做 Huber/Ridge/Patch
- 而是先有重点组，再做 A/B

### 3.2 第 3 步要比较的东西

只在重点组中比较：

1. `baseline vs patch`
2. `single vs group`
3. `ridge vs huber`
4. `absolute target vs alpha target`
5. `momentum off vs on`
6. `horizon=1 vs horizon=5`
7. `context=30/60/120/180`

### 3.3 需要改哪些代码

#### A. `src/timesfm_cn_forecast/finetuning.py`

当前主仓库里仍主要是线性残差 + `lstsq` 逻辑。

建议扩展为：
- `model_type="lstsq"`
- `model_type="ridge"`
- `model_type="huber"`

建议接口：

```python
train_linear_adapter(
    train_X,
    train_y,
    train_base,
    context_len,
    horizon_len,
    feature_names,
    stock_code=None,
    model_type="ridge",
    ridge_alpha=0.1,
    huber_epsilon=1.35,
    sample_weights=None,
)
```

这样主仓库就不用只在 `autoresearch` 里比较模型类型。

#### B. `src/timesfm_cn_forecast/features.py`

建议补两类特征：

1. 动量/趋势特征
- `momentum_5`
- `momentum_10`
- `trend_slope`
- `price_zscore`
- `vol_ratio`

2. 更规范的归一化
- 全部价格类特征尽量相对化
- 保持跨股票量纲更一致

#### C. 新增 `src/timesfm_cn_forecast/alpha.py`

建议把 alpha 目标单独抽出来，不要塞在实验脚本里。

功能：
- 计算个股相对指数的超额收益
- 支持：
  - `alpha_1d`
  - `alpha_5d`

#### D. 新增 `src/timesfm_cn_forecast/research_runner.py`

目标：
- 统一跑 A/B 实验

输入：
- group list
- top3 or top20 symbols
- `single/group`
- `ridge/huber`
- `patch on/off`
- `alpha on/off`
- `context`
- `horizon`

输出：
- `ab_summary.csv`

### 3.4 第 3 步如何迁移老仓库能力

这里可以直接借 `timesFM_fc` 的两个产物原型：

#### A. daily weights

把它迁到主仓库里，建议新增：
- `scripts/run_daily_weights.sh`
- `src/timesfm_cn_forecast/daily_weights.py`

但要改成只对“重点组前三”或“重点池”输出，而不是一上来全市场都分权。

建议输出字段：
- `date`
- `group`
- `symbol`
- `signal_score`
- `expected_return`
- `recent_stability`
- `weight`

#### B. top gainers

把老仓库 `predict_top_gainers.py` 的思路迁到主仓库，变成：
- `daily_top_candidates.csv`

但排序规则不要只看预测涨幅，还要乘上稳定性分数。

### 3.5 第 3 步要验证什么结论

最终要形成下面这些明确判断：

1. 在无泄漏回测下，`group` 是否仍优于 `single`
2. patch 在哪些组上有效，哪些组上无效
3. huber 是否在高波动组更稳
4. ridge 是否在低波动组更稳
5. alpha 目标是否更适合宽基/行业组
6. 1 天和 5 天分别适合哪些组
7. “组内前三”是否比“整个组平均”更有交易价值

### 3.6 第 3 步交付物

必须产出：
- `ab_summary.csv`
- `top3_per_group.csv`
- `daily_top_candidates.csv`
- `paper_trade_log.csv`

其中 `paper_trade_log.csv` 建议从这一步就开始做。

字段至少包括：
- `date`
- `group`
- `symbol`
- `signal_score`
- `predicted_return`
- `realized_return_1d`
- `realized_return_5d`
- `entered`
- `position_weight`

---

## 现在就可以开始改的文件清单

按优先级排序：

### 第一优先级
- `src/timesfm_cn_forecast/backtest.py`
- `src/timesfm_cn_forecast/run_group_eval.py`
- `scripts/run_single_stock_eval.sh`

### 第二优先级
- `src/timesfm_cn_forecast/finetuning.py`
- `src/timesfm_cn_forecast/features.py`
- `scripts/run_baseline_all_groups.sh`
- 新增 `scripts/run_recent_group_baseline.sh`

### 第三优先级
- 新增 `src/timesfm_cn_forecast/build_dynamic_groups.py`
- 新增 `src/timesfm_cn_forecast/daily_weights.py`
- 新增 `src/timesfm_cn_forecast/alpha.py`
- 新增 `src/timesfm_cn_forecast/research_runner.py`

---

## 现在需要补充的数据

### 必须补

1. 指数日线数据是否完整
- 为了做 alpha 目标和组级比较，需要确认 `index_market.duckdb` 中 `index_daily_data` 可用

2. 动态组定义文件
- `data/group_definitions/resonance_groups.json`
- `data/group_definitions/volatility_groups.json`
- `data/group_definitions/seed_groups.json`

3. 统一实验元数据
- 每次跑实验必须写 `meta.json`

### 建议补

1. 历史交易日历
- 用于更准确地做 `1d/5d` 对齐

2. 真实 forward 观察账本
- 哪怕先是 CSV，也要开始积累

3. 组内相关性统计缓存
- 避免每次重算全市场相关矩阵

---

## 现在最需要重新验证的结论

必须重新验证，不能直接沿用旧结论的有：

1. `group > single`
2. `patch > baseline`
3. `Huber > Ridge`
4. `1天 > 5天`
5. `动量特征一定有用`
6. `alpha target 一定有用`

这些都必须放到“无泄漏 + recent rolling”框架里重做。

---

## 推荐推进顺序

### 第 1 周

目标：
- 修掉回测切分
- 输出 recent rolling 指标

完成标准：
- 新旧结果能明确区分
- 不再出现明显不可信的满分样本

### 第 2 周

目标：
- 跑一轮 recent group baseline
- 建立 30 个重点组的候选池

完成标准：
- 产出 `group_recent_summary.csv`
- 产出每组 top3 候选

### 第 3 周

目标：
- 只对重点组做 patch / single-group / ridge-huber A/B
- 生成 daily 候选输出

完成标准：
- 产出 `ab_summary.csv`
- 产出 `daily_top_candidates.csv`
- 开始 paper trade 观察

---

## 最终要收敛成什么样

最终系统不应该是“一个万能预测器”，而应该是下面这种结构：

1. 全市场先筛组
2. 每组找最稳的 20 支
3. 每组保留最近状态最好的 3 支
4. 只对这些重点股票做次日或 5 日信号判断
5. 有强信号才进场
6. 每天记录真实 forward 表现，持续淘汰失效组

如果只用一句话总结这份执行方案：

先把评估做真，再用 baseline 找组，再在组里找最近最稳的前三支，最后才做 patch 和模型精调。
