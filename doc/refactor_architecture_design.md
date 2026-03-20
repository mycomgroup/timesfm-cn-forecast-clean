# TimesFM A股预测系统：代码重构与架构梳理设计

## 1. 目标

本设计文档的目标不是重写研究思路，而是把现有代码中已经存在的核心业务逻辑重新整理成一套更清晰、更稳定、更容易维护的工程结构。

重构后的代码应满足以下要求：

1. 让“数据读取、训练、回测、筛选、产物输出”各自有明确边界。
2. 让“单股预测、分组评估、全市场普查、种子验证、每日选股”复用同一批核心服务，而不是各写一套脚本。
3. 让时间切分、参数配置、结果文件契约成为显式对象，而不是分散在 shell 参数和临时约定里。
4. 让实验结果可以追溯：知道它用了什么数据、什么时间窗、什么参数、什么模型、什么筛选规则。

---

## 2. 当前系统的核心逻辑

从业务上看，现有系统其实已经包含了 8 条核心逻辑，只是它们现在分散在多个脚本和模块里：

1. **行情数据读取与标准化**
   - 从 `local / oss / tushare / akshare / duckdb` 读取历史数据
   - 输出统一格式的 `date / value / OHLCV`
   - 当前主要落在 `providers.py`

2. **股票池与分组管理**
   - 从 `index_market.duckdb` 读取分组
   - 从动态 JSON 注册 seed 组、vol 组、resonance 组
   - 当前主要落在 `universe/`

3. **特征工程与 Adapter 训练**
   - 根据价格和 OHLCV 计算特征
   - 训练 Ridge / Huber / lstsq 残差适配器
   - 当前主要落在 `features.py`、`finetuning.py`

4. **TimesFM 预测执行**
   - 加载基础模型
   - 执行 zero-shot 预测
   - 叠加 adapter 做修正
   - 当前主要落在 `modeling.py`

5. **回测与指标计算**
   - 滚动预测
   - 计算误差指标、交易指标、近期窗口指标
   - 当前主要落在 `backtest.py`

6. **研究筛选逻辑**
   - 组级海选
   - supernode 提取
   - companion 构建
   - 当前分散在 `run_group_eval.py`、`summarize_supernodes.py`、`build_seed_companion_groups.py`

7. **批处理编排**
   - 跑单组
   - 跑全组
   - 跑种子批次
   - 当前主要由 `scripts/*.sh` 承担

8. **研究结果产物化**
   - 批量 ranking
   - daily weights
   - alpha 计算
   - 当前主要落在 `pipeline.py`、`daily_weights.py`、`alpha.py`

这些逻辑本身是合理的，问题主要出在“边界不清”和“编排分散”。

---

## 3. 当前代码的主要结构问题

### 3.1 领域逻辑和脚本编排耦合过深

当前很多研究逻辑直接写在脚本入口里，例如：

1. shell 脚本负责定义实验参数和时间窗
2. Python 模块内部又隐含补默认值
3. 结果目录命名和文件契约由多个地方共同决定

后果是：

1. 同一个实验的真实配置很难一次看清
2. 入口一多，就容易出现“同名任务、不同口径”
3. 一个修复可能只改了某条脚本链，其他入口仍然是旧逻辑

### 3.2 时间切分逻辑分散

当前 `train_end / test_start / test_end` 的约束分散在：

1. shell 环境变量
2. `run_group_eval.py`
3. `backtest.py`
4. 种子批处理脚本

后果是：

1. 容易出现默认路径仍然泄漏
2. 某些入口有切分，另一些入口没有
3. 无法统一审计“这次实验到底是不是严格前视”

### 3.3 文件契约不统一

当前同一类结果同时存在：

1. `results.csv`
2. `group_full_results.csv`
3. `results_*.csv`
4. `meta.json`
5. 各种 task 目录下的自定义命名

后果是：

1. 汇总脚本容易读错文件
2. 同一分组多次实验的版本关系不清楚
3. 后续自动化难以稳定复用

### 3.4 预测层、评估层、筛选层没有显式分层

现在很多模块既做：

1. 数据准备
2. 模型推理
3. 指标计算
4. 最优结果选择
5. 排名输出

后果是：

1. 很难单测
2. 很难复用局部逻辑
3. 很容易在评估模块里偷偷混入筛选逻辑

### 3.5 命令行入口职责不单一

当前 `cli.py` 同时承担：

1. 单股预测
2. 批量 ranking
3. group 读取
4. 模型加载分支

这会让入口变得“能跑很多事，但没有哪件事特别干净”。

---

## 4. 重构后的总体原则

### 4.1 分层原则

建议把代码拆成四层：

1. **Domain 层**
   - 只放核心业务对象、指标定义、时间切分、结果契约
   - 不直接依赖 DuckDB、AkShare、shell、TimesFM

2. **Infrastructure 层**
   - 只负责外部世界接入
   - 如数据源、DuckDB、模型加载、文件系统、产物写入

3. **Application / Service 层**
   - 负责业务流程编排
   - 例如训练 adapter、跑回测、跑 census、生成 supernode

4. **Interface 层**
   - CLI、shell、批处理脚本
   - 只负责接参数、调用 service、打印结果

### 4.2 配置显式化原则

所有研究任务都必须显式携带：

1. 数据源配置
2. 时间切分配置
3. 模型配置
4. 特征配置
5. 评估配置
6. 输出配置

不再允许“部分靠 argparse，部分靠环境变量，部分靠内部默认值”拼出来。

### 4.3 结果契约固定化原则

建议规定每个任务目录都统一包含：

1. `spec.json`
2. `meta.json`
3. `results.csv`
4. `summary.json`
5. `logs/`

若是中间过程，再额外包含：

1. `artifacts/`
2. `adapters/`
3. `group_definitions/`

---

## 5. 推荐的目标目录结构

建议把 `src/timesfm_cn_forecast/` 重组为以下结构：

```text
src/timesfm_cn_forecast/
  domain/
    time_split.py
    experiment_spec.py
    metrics.py
    contracts.py
    ranking.py
    selection.py

  infrastructure/
    market_data/
      base.py
      providers.py
      duckdb_market.py
    universe/
      repository.py
      duckdb_universe.py
      dynamic_group_loader.py
    models/
      timesfm_loader.py
      advanced_model.py
    artifacts/
      task_store.py
      result_writer.py

  services/
    data_service.py
    training_service.py
    forecast_service.py
    backtest_service.py
    census_service.py
    supernode_service.py
    companion_service.py
    daily_signal_service.py

  workflows/
    single_forecast.py
    group_eval.py
    group_baseline.py
    census.py
    seed_eval.py
    daily_weights.py

  cli/
    main.py
    group_eval_cli.py
    census_cli.py
    seed_cli.py
    daily_cli.py
```

这不是为了“看起来高级”，而是为了把职责真正拆开。

---

## 6. 核心抽象对象

### 6.1 `TimeSplit`

负责统一表达实验时间边界。

建议字段：

```python
TimeSplit(
    train_end: str,
    discovery_start: str | None = None,
    discovery_end: str | None = None,
    validation_start: str | None = None,
    validation_end: str | None = None,
    test_start: str | None = None,
    test_end: str | None = None,
)
```

职责：

1. 校验时间顺序是否合法
2. 提供训练窗口、验证窗口、测试窗口切片方法
3. 供所有 service 共享

### 6.2 `ExperimentSpec`

负责统一表达一个实验任务的完整配置。

建议字段：

1. `task_name`
2. `mode`
3. `market_data`
4. `universe`
5. `time_split`
6. `model`
7. `features`
8. `evaluation`
9. `output`

职责：

1. 成为所有工作流的唯一输入对象
2. 作为 `spec.json` 落盘
3. 便于复现实验

### 6.3 `BacktestResult`

负责统一表达单只股票、单个 context、单个窗口的回测结果。

建议字段：

1. `symbol`
2. `group_name`
3. `context_len`
4. `eval_samples`
5. `mae / rmse / mape`
6. `directional_hit_rate`
7. `long_hit_rate`
8. `avg_trade_return`
9. `cum_return`
10. `profit_factor`
11. `max_drawdown`
12. `recent_metrics`

职责：

1. 统一所有下游汇总逻辑
2. 避免不同脚本各自拼 DataFrame

### 6.4 `TaskPaths`

负责统一任务目录结构。

建议字段：

1. `root`
2. `spec_path`
3. `meta_path`
4. `results_path`
5. `summary_path`
6. `adapters_dir`
7. `logs_dir`

职责：

1. 不再让每个脚本自己拼路径
2. 保证产物命名稳定

---

## 7. 核心服务拆分建议

### 7.1 `DataService`

职责：

1. 读取历史行情
2. 输出统一 DataFrame
3. 批量读取多股票价格矩阵
4. 在需要时执行最小数据天数过滤

替代当前散落位置：

1. `providers.py`
2. `batch_load_historical_data`
3. 各脚本中的局部加载函数

### 7.2 `TrainingService`

职责：

1. 构建训练样本
2. 训练 group adapter 或 seed adapter
3. 统一记录训练参数和样本统计

替代当前散落位置：

1. `run_group_eval.py` 中的 `_build_training_samples`
2. `finetuning.py`

### 7.3 `ForecastService`

职责：

1. 加载基础模型
2. 执行 zero-shot 预测
3. 执行 adapter 预测
4. 屏蔽 `TimesFM` 与业务层的耦合

替代当前散落位置：

1. `modeling.py`
2. `pipeline.py`

### 7.4 `BacktestService`

职责：

1. 统一滚动回测
2. 统一基准价格定义
3. 统一指标计算口径
4. 返回结构化结果对象

替代当前散落位置：

1. `backtest.py`
2. 若干脚本中的额外汇总逻辑

### 7.5 `CensusService`

职责：

1. 对单组或多组执行 discovery 评估
2. 固定使用 `ExperimentSpec + TimeSplit`
3. 产出标准化 `results.csv`

替代当前散落位置：

1. `run_group_eval.py`
2. `run_group_baseline.py`
3. `run_all_groups_eval.sh`
4. `run_census_task.sh`

### 7.6 `SupernodeService`

职责：

1. 基于 discovery 结果提取候选 seed
2. 基于 validation 结果确认 seed
3. 固定 ranking 规则和阈值规则

替代当前散落位置：

1. `summarize_supernodes.py`
2. 与 supernode 相关的散落 DataFrame 处理

### 7.7 `CompanionService`

职责：

1. 构建 companion 候选池
2. 计算相关性、同步率、波动相似度
3. 生成 companion 变体
4. 输出冻结后的 group definitions

替代当前散落位置：

1. `build_seed_companion_groups.py`
2. `build_dynamic_groups.py`

### 7.8 `DailySignalService`

职责：

1. 批量预测
2. ranking
3. alpha 计算
4. daily weights 生成

替代当前散落位置：

1. `pipeline.py` 的批量部分
2. `alpha.py`
3. `daily_weights.py`

---

## 8. 推荐的工作流划分

建议不再让 shell 脚本承载业务逻辑，而是让 shell 只做“薄包装”。

### 8.1 单股预测工作流

输入：

1. `ExperimentSpec`
2. `symbol`

输出：

1. `history.csv`
2. `forecast.csv`
3. `summary.json`
4. `forecast.png`

### 8.2 单组评估工作流

输入：

1. `ExperimentSpec`
2. `group_name`

输出：

1. `results.csv`
2. `group_top3_summary.csv`
3. `meta.json`

### 8.3 普查工作流

输入：

1. `ExperimentSpec`
2. `group_list`

输出：

1. 每组一个标准 task 目录
2. 一份总表 `group_summary.csv`

### 8.4 Seed 验证工作流

输入：

1. 冻结的 seed 名单
2. companion 定义
3. validation / final-test 配置

输出：

1. `seed_group_compare.csv`
2. `best_seed_group.json`

### 8.5 Daily 输出工作流

输入：

1. 已冻结的优选组
2. 最新行情
3. zero-shot 或 adapter 模型

输出：

1. `daily_picks.csv`
2. `daily_weights.csv`

---

## 9. 当前文件到目标结构的映射建议

### 9.1 可直接保留并迁移的模块

1. [src/timesfm_cn_forecast/providers.py](/Users/fengzhi/Downloads/git/timesfm-cn-forecast-clean/src/timesfm_cn_forecast/providers.py)
   - 迁移到 `infrastructure/market_data/providers.py`

2. [src/timesfm_cn_forecast/features.py](/Users/fengzhi/Downloads/git/timesfm-cn-forecast-clean/src/timesfm_cn_forecast/features.py)
   - 迁移到 `services/training` 相关模块或 `domain/features.py`

3. [src/timesfm_cn_forecast/finetuning.py](/Users/fengzhi/Downloads/git/timesfm-cn-forecast-clean/src/timesfm_cn_forecast/finetuning.py)
   - 拆成 `domain/contracts.py` + `services/training_service.py`

4. [src/timesfm_cn_forecast/modeling.py](/Users/fengzhi/Downloads/git/timesfm-cn-forecast-clean/src/timesfm_cn_forecast/modeling.py)
   - 拆成 `infrastructure/models/timesfm_loader.py` + `infrastructure/models/advanced_model.py`

5. [src/timesfm_cn_forecast/backtest.py](/Users/fengzhi/Downloads/git/timesfm-cn-forecast-clean/src/timesfm_cn_forecast/backtest.py)
   - 拆成 `domain/metrics.py` + `services/backtest_service.py`

6. [src/timesfm_cn_forecast/universe/](/Users/fengzhi/Downloads/git/timesfm-cn-forecast-clean/src/timesfm_cn_forecast/universe/__init__.py)
   - 整体迁移到 `infrastructure/universe/`

### 9.2 建议逐步退役的“大杂烩入口”

1. [src/timesfm_cn_forecast/cli.py](/Users/fengzhi/Downloads/git/timesfm-cn-forecast-clean/src/timesfm_cn_forecast/cli.py)
   - 改成顶层路由 CLI

2. [src/timesfm_cn_forecast/pipeline.py](/Users/fengzhi/Downloads/git/timesfm-cn-forecast-clean/src/timesfm_cn_forecast/pipeline.py)
   - 拆成单股 workflow 和 daily ranking workflow

3. [src/timesfm_cn_forecast/run_group_eval.py](/Users/fengzhi/Downloads/git/timesfm-cn-forecast-clean/src/timesfm_cn_forecast/run_group_eval.py)
   - 从“脚本式模块”改成 `GroupEvaluationWorkflow`

4. `scripts/*.sh`
   - 保留极少量包装层
   - 主逻辑迁到 Python workflow

---

## 10. 建议的重构顺序

### 阶段 A：先收口契约，不动业务逻辑

目标：

1. 引入 `ExperimentSpec`
2. 引入 `TimeSplit`
3. 引入 `TaskPaths`
4. 固定 `results.csv / summary.json / meta.json`

收益：

1. 不需要大改算法
2. 先把“配置”和“产物”稳定下来

### 阶段 B：拆评估链路

目标：

1. 把 `backtest.py` 拆成纯指标函数和回测服务
2. 把 `run_group_eval.py` 变成 workflow
3. 让 baseline 和 adapter 共用同一回测引擎

收益：

1. 统一回测口径
2. 统一时间切分逻辑

### 阶段 C：拆筛选链路

目标：

1. 把 supernode 提取变成 `SupernodeService`
2. 把 companion 构建变成 `CompanionService`
3. 显式区分 discovery / validation / final-test

收益：

1. 研究逻辑会比现在清晰很多
2. 更容易防泄漏

### 阶段 D：拆产物层

目标：

1. 把 `pipeline.py` 中的单股预测和批量 ranking 拆开
2. 把 `alpha.py`、`daily_weights.py` 纳入统一 daily workflow

收益：

1. 输出层更清晰
2. 日常任务和研究任务不会继续混在一起

---

## 11. 测试策略

重构时建议至少补三类测试。

### 11.1 纯函数测试

覆盖对象：

1. 指标计算
2. 时间切分校验
3. ranking 规则
4. companion 打分规则

### 11.2 契约测试

覆盖对象：

1. `results.csv` 列名
2. `summary.json` 字段
3. `meta.json` 字段
4. 动态分组 JSON 格式

### 11.3 小样本集成测试

覆盖对象：

1. 单组 zero-shot 评估
2. 单组 adapter 评估
3. supernode 提取
4. companion 生成

要求：

1. 使用小型测试股票池
2. 使用固定日期
3. 可以在本地快速跑完

---

## 12. 重构后最重要的收益

如果按这个设计推进，最大的收益不是“代码更优雅”，而是：

1. **研究结论更可信**
   - 因为时间切分、筛选阶段、最终测试都被显式化了

2. **代码更容易改**
   - 你以后改 companion 规则，不必再去碰回测引擎

3. **脚本更少，但能力更强**
   - shell 只做入口包装，真正逻辑都在 Python workflow

4. **结果更容易复盘**
   - 每次实验都能回答：用了什么参数、什么时间窗、什么股票池、输出在哪里

---

## 13. 建议的第一步落地范围

如果只做第一轮重构，我建议先只做这 4 件事：

1. 抽出 `TimeSplit + ExperimentSpec + TaskPaths`
2. 把 `backtest.py` 拆成纯指标层和回测服务层
3. 把 `run_group_eval.py` 改成使用统一 `results.csv / meta.json`
4. 把 `run_all_groups_eval.sh`、`run_census_task.sh` 改成薄包装

这四步完成后，整个工程就会开始“有主干”。

后续再拆 supernode 和 companion，会顺很多。
