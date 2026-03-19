# 种子股专属陪跑组功能工作项

更新时间：2026-03-19

这份文档不是讲理念，而是把“专属陪跑组”功能拆成可执行工作项。

目标：

- 先打通主链路
- 再补生成器
- 再补专项比较
- 每一步都能独立验收

---

## 0. 交付目标

最终要交付的能力是：

1. `run_group_eval.py` 能直接识别：
   - DuckDB 里的普通组
   - `data/group_definitions/*.json` 里的动态组
   - `single_<symbol>` 合成组

2. 系统能为每只种子股生成多个候选陪跑组

3. 系统能对每只种子股输出：
   - `single`
   - 原始组
   - 专属组
   的统一比较结果

4. 系统能输出一份明确的最优组结论

---

## 1. P0：打通组解析主链路

### 1.1 新增统一 resolver

工作项：

- 新增文件 `src/timesfm_cn_forecast/universe/resolver.py`
- 提供接口：
  - `resolve_group_symbols(group_name, duckdb_path, group_definitions_dir="data/group_definitions")`
  - `group_exists(...)`
  - `resolve_group_source(...)`

解析优先级：

1. `single_<symbol>`
2. DuckDB 组
3. `data/group_definitions/*.json`
4. 抛出明确错误

验收标准：

- 输入 `HS300` 能返回股票池
- 输入 `resonance_sh600519` 能返回股票池
- 输入 `single_600519` 能返回 `["600519"]`
- 输入不存在的组会报出可读错误

### 1.2 让主训练链路改用 resolver

工作项：

- 修改 `src/timesfm_cn_forecast/run_group_eval.py`
- 修改 `src/timesfm_cn_forecast/run_group_baseline.py`
- 把当前 `get_stock_universe()` 调用替换为 resolver

验收标准：

- 旧组跑法不变
- 动态组不写库也能直接训练
- 空组时报错信息明确指出组来源解析失败

### 1.3 让 `single_<symbol>` 成为一等公民

工作项：

- resolver 中内置 `single_<symbol>` 规则
- 对 symbol 做合法性校验和归一化

验收标准：

- `scripts/run_ab_test.sh` 不依赖预先写库
- `single_2673` 这种输入能直接解析为 `002673`
  - 若业务上希望保留原始输入，也需明确规则

注意：

- 这里必须统一 symbol 规范化逻辑，避免 `2673`、`002673`、`sz002673` 混用

### 1.4 动态组 JSON 读取能力补测试

工作项：

- 新增测试文件
  - `tests/universe/test_group_resolver.py`

测试覆盖：

- dict 结构 JSON
- `groups` list 结构 JSON
- `single_<symbol>`
- DuckDB 组优先级
- 不存在组的错误信息

验收标准：

- 解析相关测试全部通过

---

## 2. P1：实现专属陪跑组生成器

### 2.1 新增候选组构造脚本

工作项：

- 新增 `scripts/build_seed_companion_groups.py`

输入：

- `seed_candidates.csv` 或 seed 列表文件
- `market.duckdb`
- 可选已有组结果目录

输出：

- `data/group_definitions/seed_groups.json`
- `data/tasks/<task>/seed_group_build_report.csv`

第一阶段建议输出的候选组类型：

- `corr_top10`
- `corr_top20`
- `resonance_trimmed`
- `hybrid_v1`

验收标准：

- 给定 3 只种子股，能稳定生成多组候选组
- 输出文件中带有 `seed`、`name`、`symbols`、`source`、`version`

### 2.2 统一组定义格式

工作项：

- 明确 `data/group_definitions/*.json` 标准 schema
- 所有新生成脚本都遵循同一结构

建议字段：

- `name`
- `seed`
- `source`
- `version`
- `symbols`
- 可选 `build_meta`

验收标准：

- 生成的组定义可直接被 resolver 读取

### 2.3 补全构组元数据

工作项：

- 在生成报告里记录：
  - 构组窗口
  - 构组方法
  - TopK
  - 原始来源组
  - 与种子股的相关性摘要

验收标准：

- 任意一个候选组都能追溯“它是怎么来的”

---

## 3. P2：实现种子股专项评估器

### 3.1 新增 seed runner

工作项：

- 新增 `src/timesfm_cn_forecast/run_seed_group_eval.py`
或
- 新增 `scripts/run_seed_group_eval.sh`

输入：

- `seed_symbol`
- `origin_group`
- `candidate_groups`
- 统一训练/测试窗口

输出：

- 每个候选组的标准结果目录
- `seed_comparison_report.csv`
- `seed_best_group.json`

验收标准：

- 对单只种子股，能自动比较：
  - `single`
  - `origin_group`
  - 多个 `seed_group`

### 3.2 聚焦“种子股本人”结果

工作项：

- 在每个候选组结果中，只抽取目标 seed 本人的那一行
- 合成种子专项比较表

建议输出字段：

- `seed_symbol`
- `candidate_group`
- `candidate_type`
- `hitrate`
- `trade_score`
- `recent20_avg_ret`
- `profit_factor`
- `max_drawdown`
- `supernode_candidate`

验收标准：

- 比较表一眼能看出哪种训练方式对 seed 最优

### 3.3 输出最优组结论

工作项：

- 设计种子组排序规则

建议排序优先级：

1. `trade_score`
2. `recent20_avg_ret`
3. `hitrate`
4. `profit_factor`
5. `max_drawdown`

验收标准：

- 每只种子股都会产出唯一的 `best_group`

---

## 4. P3：汇总与研究闭环

### 4.1 新增汇总脚本

工作项：

- 新增 `scripts/summarize_seed_companion_results.py`

输出：

- `best_seed_groups.csv`
- `seed_strategy_scoreboard.csv`
- `cross_seed_repeat_peers.csv`

验收标准：

- 能批量汇总多只种子股的最优专属组

### 4.2 产出可复用研究资产

工作项：

- 将最优组写回：
  - `data/group_definitions/best_seed_groups.json`
或
  - `data/tasks/.../best_seed_groups.csv`

验收标准：

- 后续日常预测可以直接复用这份最优组定义

---

## 5. 横向清理项

### 5.1 symbol 规范化

工作项：

- 统一接受：
  - `600519`
  - `sh600519`
  - `sz300079`
- 内部训练与评估一律转成 `6` 位纯代码

验收标准：

- JSON、DuckDB、回测输出三者 symbol 一致

### 5.2 日志与进度输出

工作项：

- 对长任务增加更清晰的进度输出
- 至少打印：
  - 当前 seed
  - 当前候选组
  - 当前阶段
  - 已完成数 / 总数

验收标准：

- 跑批过程中能快速判断是在训练、回测还是卡住

### 5.3 元数据统一

工作项：

- 所有实验任务都写 `meta.json`
- 增加：
  - `seed_symbol`
  - `candidate_type`
  - `group_source`
  - `group_definition_file`

验收标准：

- 每个任务目录都可复盘

---

## 6. 测试与验证清单

### 6.1 单元测试

- resolver 对 DuckDB 组解析正确
- resolver 对 JSON 动态组解析正确
- resolver 对 `single_<symbol>` 解析正确
- symbol 规范化正确

### 6.2 集成测试

- `run_group_eval --group single_600519` 能完成
- `run_group_eval --group resonance_sh600519` 能完成
- `run_group_eval --group seed_600519_corr_top10` 能完成

### 6.3 业务验证

至少验证下面三类结果：

1. `single`
2. 原始共振组
3. 专属陪跑组

业务验收标准：

- 至少 1 只种子股能完整跑通三者对比
- 结果表明确列出 seed 本人的最优方案

---

## 7. 建议开发顺序

### 第 1 周

- 完成 resolver
- 接入 `run_group_eval.py`
- 打通 `single_<symbol>`

### 第 2 周

- 完成 `build_seed_companion_groups.py`
- 生成第一批种子组

### 第 3 周

- 完成 seed 专项 runner
- 产出 `single vs 原始组 vs 专属组` 比较表

### 第 4 周

- 完成汇总脚本
- 形成第一版最优专属陪跑组清单

---

## 8. 文件改动清单

### 必改文件

- `src/timesfm_cn_forecast/run_group_eval.py`
- `src/timesfm_cn_forecast/run_group_baseline.py`

### 建议新增文件

- `src/timesfm_cn_forecast/universe/resolver.py`
- `scripts/build_seed_companion_groups.py`
- `src/timesfm_cn_forecast/run_seed_group_eval.py`
- `scripts/summarize_seed_companion_results.py`
- `tests/universe/test_group_resolver.py`

### 建议新增数据文件

- `data/group_definitions/seed_groups.json`
- `data/group_definitions/best_seed_groups.json`

---

## 9. P0 完成定义

只有满足下面四条，才算 P0 真正完成：

1. `single_<symbol>` 可以直接跑，不依赖预写库
2. `data/group_definitions/*.json` 中的新组可以直接跑
3. `run_group_eval.py` 旧组能力不回退
4. 解析与训练链路有最小测试覆盖

---

## 10. 一句话优先级

最先做的不是“更聪明的陪跑组算法”，而是“让组定义真正进入训练主链路”。

只要这一步打通，后面的专属陪跑组生成和专项评估就能快速叠上去。
