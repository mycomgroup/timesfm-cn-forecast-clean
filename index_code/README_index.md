# 股票指数数据管理系统

基于股票数据更新系统，专门为股票指数数据设计的完整管理解决方案。

## 系统特点

### 🎯 专业指数数据处理
- 支持上交所和深交所所有指数
- 自动识别指数类型和分类
- 完整的历史数据导入
- 智能增量更新机制

### 📊 数据质量保证
- 严格的数据验证规则
- 重复数据自动去除
- 异常数据检测和处理
- 完整的操作日志记录

### 🚀 高效批量处理
- 批量SQL操作优化
- 智能内存管理
- 并发控制和限流
- 错误恢复机制

## 文件结构

```
index_code/
├── DESIGN.md              # 系统设计文档
├── index_ingest.py        # 历史数据批量导入
├── index_update.py        # 增量更新程序
├── index_manager.py       # 主控制程序
├── check_index_db.py      # 数据库状态检查
└── README_index.md        # 使用说明文档
```

## 数据库表结构

### 主数据表 (index_daily_data)
存储指数的日线数据，包含开高低收成交量等信息。

### 指数信息表 (index_info)
存储指数的基本信息，如名称、交易所、分类等。

### 导入日志表 (index_import_log)
记录历史数据导入的详细日志。

### 更新日志表 (index_update_log)
记录增量更新的操作日志。

## 使用方法

### 1. 初始化 - 导入历史数据

首次使用时，需要导入历史CSV文件：

```bash
# 导入所有历史数据
python3 index_code/index_manager.py import

# 限制导入文件数量（测试用）
python3 index_code/index_manager.py import --limit 10

# 指定自定义路径
python3 index_code/index_manager.py import \
    --source-dir /path/to/csv/files \
    --db-path /path/to/database.duckdb
```

### 2. 日常维护 - 增量更新

定期运行增量更新，获取最新数据：

```bash
# 更新所有指数
python3 index_code/index_manager.py update

# 限制更新数量（测试用）
python3 index_code/index_manager.py update --limit 5

# 设置请求间隔（避免限流）
python3 index_code/index_manager.py update --sleep 2.0
```

### 3. 状态检查 - 数据库监控

检查数据库状态和数据质量：

```bash
# 检查数据库状态
python3 index_code/index_manager.py check

# 检查指定数据库
python3 index_code/check_index_db.py /path/to/database.duckdb
```

## 高级用法

### 环境变量控制

```bash
# 限制处理文件数量
export LIMIT_FILES=50
export LIMIT_INDICES=20

# 设置请求间隔
export SLEEP_SECONDS=1.5

# 然后运行程序
python3 index_code/index_manager.py update
```

### 试运行模式

```bash
# 试运行，不实际执行操作
python3 index_code/index_manager.py update --dry-run
```

## 支持的指数类型

### 上交所指数 (sh开头)
- **主要指数**: 上证指数(000001)、上证50(000016)、沪深300(000300)
- **行业指数**: 各行业分类指数
- **主题指数**: 各种主题投资指数
- **策略指数**: 价值、成长、红利等策略指数

### 深交所指数 (sz开头)
- **主要指数**: 深证成指(399001)、创业板指(399006)
- **中小板指数**: 中小板相关指数
- **创业板指数**: 创业板相关指数
- **行业指数**: 深交所行业分类指数

### 特殊指数
- **深证特殊**: sz970xxx、sz980xxx系列

## 数据质量监控

### 自动验证规则
- ✅ 日期格式验证 (YYYY-MM-DD)
- ✅ 数值字段非空检查
- ✅ 价格数据合理性验证
- ✅ 重复数据自动去除

### 异常处理
- 🔄 网络请求自动重试
- ⚠️ 数据异常自动跳过
- 📝 详细错误日志记录
- 🔧 失败任务状态跟踪

## 性能优化

### 批量处理优化
- 批量SQL插入操作
- 事务管理优化
- 内存使用控制
- 临时文件自动清理

### 并发控制
- 请求频率智能限制
- 数据库连接池管理
- 资源锁定机制
- 优雅的错误恢复

## 监控指标

### 数据完整性
- 指数数量统计
- 数据记录总数
- 日期范围覆盖
- 交易所分布情况

### 更新状态
- 最近更新时间
- 更新成功率
- 新增数据量
- 异常指数识别

### 系统性能
- 处理速度统计
- 内存使用情况
- 存储空间占用
- 网络请求延迟

## 故障排除

### 常见问题

**1. 数据库文件不存在**
```bash
# 解决方案：先运行导入模式
python3 index_code/index_manager.py import
```

**2. 网络请求失败**
```bash
# 解决方案：增加请求间隔
python3 index_code/index_manager.py update --sleep 3.0
```

**3. 数据格式异常**
```bash
# 解决方案：检查源数据文件格式
python3 index_code/index_manager.py check
```

### 日志分析

程序运行时会输出详细日志，包括：
- 📊 处理进度信息
- ✅ 成功操作记录
- ❌ 错误详细信息
- 📈 性能统计数据

### 数据恢复

如果数据出现问题，可以：
1. 备份当前数据库
2. 重新运行导入程序
3. 检查数据完整性
4. 恢复增量更新

## 最佳实践

### 1. 初始化建议
- 首次运行建议使用小批量测试
- 确认数据格式正确后再全量导入
- 定期备份数据库文件

### 2. 日常维护
- 建议每日运行一次增量更新
- 设置合适的请求间隔避免限流
- 定期检查数据库状态

### 3. 监控告警
- 监控更新成功率
- 关注异常指数数量
- 跟踪数据延迟情况

### 4. 性能调优
- 根据网络情况调整请求间隔
- 合理设置批量处理大小
- 定期清理临时文件

## 依赖环境

### Python包依赖
```bash
pip install duckdb akshare pandas
```

### 系统要求
- Python 3.9+
- 足够的磁盘空间（建议10GB+）
- 稳定的网络连接
- 充足的内存（建议4GB+）

## 技术支持

如遇到问题，请检查：
1. 📋 程序运行日志
2. 🗃️ 数据库状态报告
3. 🌐 网络连接状况
4. 💾 磁盘空间情况

---

**注意**: 本系统专为指数数据设计，与股票数据系统相互独立，可以同时运行而不会冲突。