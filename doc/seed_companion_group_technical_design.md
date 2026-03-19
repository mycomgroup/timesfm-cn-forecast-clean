# 种子股专属陪跑组功能技术设计

更新时间：2026-03-19

这份文档回答三个问题：

1. 现有仓库是否已经具备“专属陪跑组”能力。
2. 需要新增哪些能力，才能把这件事做成完整闭环。
3. 建议按什么架构落地，既复用现有能力，又不把主链路改乱。

---

## 1. 背景与目标

当前仓库已经证明了一个重要现象：

- 组均胜率通常只有 `42%-45%`
- 但组内会稳定抬升少数“超级节点”
- 某些种子股在特定共振组中能达到 `52%-55%` 胜率

下一步的核心目标，不再是继续找“更大的好组”，而是围绕每一只高价值种子股，构造它自己的“专属陪跑组”。

这里的“专属陪跑组”定义为：

- 以某只种子股为中心
- 从历史价格行为、现有共振结果、行业概念关系中筛出一小组最有助于训练稳定性的陪跑股票
- 用这组股票联合训练 adapter
- 最终只关心该种子股本人的预测与交易表现是否优于：
  - `single`
  - 原始大组
  - 其他候选陪跑组版本

最终希望形成一条完整链路：

1. 发现种子股
2. 为每只种子股生成候选陪跑组
3. 训练并评估多个候选组
4. 选出该种子股的最优专属陪跑组
5. 产出可复用的组定义与比较报告

---

## 2. 当前代码现状

### 2.1 已具备的能力

现有仓库已经有三块重要基础设施：

1. 组训练与组回测主引擎
   - `src/timesfm_cn_forecast/run_group_eval.py`
   - 已支持：
     - 按组取股票池
     - 统一训练 adapter
     - 逐股回测
     - 输出 `group_full_results.csv`
     - 输出 `group_top3_summary.csv`
     - 输出 `meta.json`

2. 动态组定义文件格式
   - `src/timesfm_cn_forecast/universe/fetcher.py`
   - 已支持从 `data/group_definitions/*.json` 读取动态组
   - 已支持两种 JSON 结构：
     - `{ "group_name": ["000001", "000002"] }`
     - `{ "groups": [{ "name": "...", "symbols": [...] }] }`

3. 动态共振/seed 组构造脚本雏形
   - `src/timesfm_cn_forecast/build_dynamic_groups.py`
   - 已支持按 seed 生成：
     - `resonance_groups.json`
     - `seed_groups.json`
     - `volatility_groups.json`

### 2.2 当前真正的断点

虽然基础能力存在，但主流程并没有完全打通。

#### 断点 A：训练入口只认 DuckDB 中已有组

`get_stock_universe()` 当前只调用 `query_constituents()` 从 DuckDB 读取组成员。

这意味着：

- 如果 `seed_600519` 只存在于 `data/group_definitions/seed_groups.json`
- 但没有先写入 `index_market.duckdb`
- 那么 `run_group_eval.py --group seed_600519` 仍然会拿不到股票池

结论：

- “动态组定义已支持”
- 但“训练主链路自动识别动态组”尚未支持

#### 断点 B：CLI 不接受未知动态组

`src/timesfm_cn_forecast/universe/cli.py` 当前只允许处理 `INDEX_MAP` 中已注册的组。

这意味着：

- `resonance_*`、`seed_*`、`single_*` 这类动态组
- 不能通过现有 CLI 自然地拉取/写入/刷新

#### 断点 C：`single_<symbol>` 没有一等公民支持

当前 `scripts/run_ab_test.sh` 会直接调用：

```bash
--group "single_$symbol"
```

但数据库里并没有 `single_*` 组的自动生成逻辑。

这导致：

- A/B 流程逻辑上是成立的
- 但依赖“外部先把 `single_*` 写进库”这个隐含前提
- 稳定性不足，容易假死或直接拿空组

#### 断点 D：还没有“种子视角”的专项评估器

现有 `run_group_eval.py` 评估的是“整个组里所有股票”。

但专属陪跑组功能真正要回答的问题是：

- 某个候选组是否对种子股本人更有帮助

所以需要专门的比较器，只聚焦：

- `seed_symbol`
- `single`
- `origin_group`
- `candidate_seed_group_v1/v2/v3`

#### 断点 E：组定义命名与代码格式不统一

当前仓库内同时出现：

- `600519`
- `sh600519`
- `sz300079`

如果不先统一，后面会在这些场景持续踩坑：

- 动态组生成
- JSON 存储
- DuckDB 写入
- 回测 symbol 对齐
- 种子股报告汇总

---

## 3. 功能范围

### 3.1 本次要做的范围

本功能要覆盖以下闭环：

1. 支持“按组名直接解析股票池”，无论来源于：
   - DuckDB
   - `data/group_definitions/*.json`
   - 程序内置的 `single_<symbol>` 合成组

2. 支持为每只种子股生成一个或多个专属陪跑组定义

3. 支持批量评估：
   - `single`
   - 原始共振/行业/概念大组
   - 专属陪跑组

4. 支持只从“种子股本人表现”角度进行排序和决策

5. 输出标准化元数据和比较报告，便于复盘与实盘使用

### 3.2 本次不做的范围

以下内容不作为第一阶段必须项：

- 图形化界面
- 自动实盘下单
- 全市场所有股票每天自动重建专属组
- 复杂图算法或 GNN 建模
- 多目标贝叶斯优化

---

## 4. 目标架构

### 4.1 核心设计原则

1. 训练主引擎尽量不重写
2. 组解析能力前移，统一入口
3. 组定义尽量文件化，可追溯、可复用
4. 比较逻辑单独抽出，避免污染 `run_group_eval.py`
5. `single`、`原始大组`、`专属组` 尽量走同一评估口径

### 4.2 模块划分

建议新增或调整为下面四层：

#### 第 1 层：组解析层

建议新增统一接口，例如：

```python
resolve_group_symbols(group_name: str, duckdb_path: str) -> list[str]
```

解析顺序建议为：

1. 若 `group_name` 是 `single_<symbol>`
   - 直接返回 `[symbol]`
2. 若 DuckDB 中存在该组
   - 读取 DuckDB
3. 若 `data/group_definitions/*.json` 中存在该组
   - 读取 JSON
4. 否则报错

这一层要成为：

- `run_group_eval.py`
- `run_group_baseline.py`
- `daily_predict`
- 后续种子组 runner

共同依赖的唯一股票池入口。

#### 第 2 层：专属陪跑组构造层

建议新增脚本：

- `scripts/build_seed_companion_groups.py`

输入：

- 种子股列表
- 历史价格数据
- 已有组结果
- 可选的行业/概念先验

输出：

- `data/group_definitions/seed_groups.json`
- 可选拆分文件：
  - `data/group_definitions/seed_600519.json`
  - `data/group_definitions/seed_300079.json`

建议每只种子股输出多个候选组版本，例如：

- `seed_600519_corr_top10`
- `seed_600519_corr_top20`
- `seed_600519_resonance_trimmed`
- `seed_600519_hybrid_v1`

这样后续评估时可以横向比较。

#### 第 3 层：种子专项评估层

建议新增脚本：

- `scripts/run_seed_group_eval.sh`
或
- `src/timesfm_cn_forecast/run_seed_group_eval.py`

输入：

- `seed_symbol`
- 原始组列表
- 候选专属组列表
- 统一训练/测试时间窗口

输出：

- 每个候选组的完整组结果
- 单独抽出的种子股比较表

建议核心输出文件：

- `seed_comparison_report.csv`
- `seed_best_group.json`
- `seed_group_scoreboard.csv`

#### 第 4 层：汇总与决策层

建议新增汇总脚本：

- `scripts/summarize_seed_companion_results.py`

汇总维度：

- 每只种子股最优组
- 不同候选组版本对比
- `single vs 原始组 vs 专属组`
- 最近收益表现
- 横跨多个种子股的重复陪跑节点

---

## 5. 数据模型设计

### 5.1 组定义文件格式

建议统一采用下面的结构：

```json
{
  "groups": [
    {
      "name": "seed_600519_corr_top10",
      "seed": "600519",
      "source": "seed_companion_builder",
      "version": "v1",
      "symbols": ["600519", "000568", "000596", "600529"]
    }
  ]
}
```

相比简单字典格式，这种结构更适合承载元信息：

- `seed`
- `source`
- `version`
- 后续可扩展：
  - `score`
  - `build_window`
  - `build_method`

### 5.2 比较报告格式

建议种子专项比较至少包含这些字段：

- `seed_symbol`
- `candidate_group`
- `candidate_type`
  - `single`
  - `origin_group`
  - `seed_group`
- `origin_group_name`
- `hitrate`
- `trade_score`
- `recent20_avg_ret`
- `profit_factor`
- `max_drawdown`
- `supernode_candidate`
- `rank_within_seed`

### 5.3 元数据要求

每次评估都必须写 `meta.json`，至少包含：

- `seed_symbol`
- `group_name`
- `candidate_type`
- `build_method`
- `train_end`
- `test_start`
- `test_end`
- `feature_set`
- `model_type`
- `train_days`
- `context_len`
- `group_source`

---

## 6. 算法与构组策略

### 6.1 第一阶段建议的简单可用策略

先不要把构组算法做得太复杂，第一阶段采用可解释、可复盘的规则法即可。

建议对每只种子股生成 3 类候选组：

1. 纯相关性组
   - 过去 N 天收益率相关系数 TopK

2. 共振修剪组
   - 从原始 `resonance_<seed>` 组中
   - 只保留：
     - 最近表现更好的成员
     - 与种子股重复同向性更强的成员

3. 混合组
   - 共振成员
   - 行业/概念近邻
   - 历史上在多个强组里重复出现的超级节点

### 6.2 第一阶段不建议直接做的复杂策略

以下策略建议留到第二阶段：

- 聚类后再做子簇搜索
- 图网络嵌入
- 多目标遗传算法选组
- 用回测结果反向训练组生成模型

原因很简单：

- 当前最缺的不是模型复杂度
- 而是稳定、可重复、能复盘的实验闭环

---

## 7. 代码改动建议

### 7.1 必改项

#### A. 统一股票池解析入口

建议新增：

- `src/timesfm_cn_forecast/universe/resolver.py`

提供：

- `resolve_group_symbols()`
- `group_exists()`
- `load_group_definition()`

并让以下代码改为依赖这一层：

- `run_group_eval.py`
- `run_group_baseline.py`
- 未来的 `run_seed_group_eval.py`

#### B. 修复动态组无法直接进入训练主链路的问题

`run_group_eval.py` 当前调用 `get_stock_universe()` 的方式需要替换成统一 resolver。

#### C. 给 `single_<symbol>` 做内置解析

这样 A/B test 不再依赖“预先写库”。

#### D. 提供种子股专项 runner

这是把“能跑组实验”升级为“能回答种子股最优方案”的关键。

### 7.2 建议项

#### A. CLI 支持动态组 materialize

建议让 `python -m timesfm_cn_forecast.universe --index seed_600519_corr_top10`
也能工作。

#### B. 支持 group source 标识

例如：

- `db`
- `dynamic_json`
- `synthetic_single`

方便排查与复盘。

#### C. 统一 symbol 规范化

建议内部统一使用：

- 外部 JSON 可接受 `sh600519/sz300079/600519`
- 内部训练和评估一律使用 `6` 位纯代码

---

## 8. 输出目录设计

建议新增任务目录规范：

```text
data/tasks/seed_companion_20260319_230000/
  seed_candidates.csv
  seed_group_build_report.csv
  seed_groups/
    seed_600519_corr_top10/
      group_full_results.csv
      group_top3_summary.csv
      meta.json
    seed_600519_resonance_trimmed/
      ...
  seed_reports/
    600519/
      seed_comparison_report.csv
      seed_best_group.json
    300079/
      ...
```

这样后面查一只种子股的完整实验链条会非常直接。

---

## 9. 兼容性与迁移策略

### 9.1 对现有实验的兼容性

本方案应保证以下旧能力不受破坏：

- `HS300`、`ZZ500`、`ind_*`、`con_*` 正常跑
- 已经写进 DuckDB 的 `resonance_*` 正常跑
- 旧的 `run_group_eval.py` 参数不变

### 9.2 迁移策略

建议采用“增量接入”：

1. 先只新增 resolver，不动实验口径
2. 再新增 `single_<symbol>` 支持
3. 再新增 `seed_groups.json` 解析
4. 最后新增 seed 专项 runner

这样每一步都能独立验证，不会把整个研究链路一次性改炸。

---

## 10. 风险与注意事项

### 10.1 最大风险

最大的风险不是代码复杂，而是评估失真。

专属陪跑组功能最容易出现三类伪提升：

1. 用全样本相关性构组，再回测同一时间窗口
2. 先看结果再选组，产生幸存者偏差
3. 不同候选组的训练/测试切分不一致

所以必须保证：

- 构组窗口、训练窗口、测试窗口严格分开
- 种子组比较使用完全相同的训练/测试参数
- 组生成过程写入元数据，避免复盘时找不到版本

### 10.2 次要风险

- 动态组过大，算力压力上升
- 组内混入与种子股无关的噪音票
- 不同文件中的 symbol 格式不统一
- 当前日志/进度输出不够细，长任务排障困难

---

## 11. 分阶段落地建议

### P0：打通主链路

目标：

- `run_group_eval --group seed_xxx` 不需要手工灌库也能直接跑
- `single_<symbol>` 自动可用

### P1：做种子专属组生成器

目标：

- 能批量为候选种子股生成专属组 JSON

### P2：做种子专项比较器

目标：

- 自动回答“single / 原始组 / 专属组”谁最好

### P3：做汇总与研究闭环

目标：

- 批量输出每只种子股的最优专属陪跑组
- 形成可迭代研究资产

---

## 12. 一句话结论

这项功能不需要推翻现有仓库。

最优做法是：

- 保留 `run_group_eval.py` 作为训练与回测主引擎
- 新增统一组解析层
- 新增种子组构造脚本
- 新增种子专项比较 runner

这样就能把“已经验证有效的共振研究”升级成“每只超级节点都能拥有自己的专属陪跑组”的完整系统。
