# DuckDB股票数据更新程序

基于现有的 `te.py` 和 `duckdb_ingest.py` 代码，创建了一套完整的股票数据更新系统。

## 文件说明

### 1. `duckdb_update.py` - 增量更新程序
- 检查数据库中现有股票的最新日期
- 只下载和更新缺失的新数据
- 避免重复导入，提高效率

### 2. `add_new_stocks.py` - 新股票添加程序  
- 自动发现市场上的新股票
- 下载新股票的完整历史数据
- 添加到数据库中

### 3. `update_manager.py` - 主控制程序
- 统一的命令行界面
- 支持多种更新模式
- 提供参数控制和日志记录

## 使用方法

### 基本用法

```bash
# 增量更新现有股票数据
python update_manager.py update

# 添加新股票
python update_manager.py add-new

# 全量更新（新股票 + 增量更新）
python update_manager.py full
```

### 高级参数

```bash
# 指定数据库路径
python update_manager.py update --db-path /path/to/your/market.duckdb

# 限制处理数量（用于测试）
python update_manager.py update --limit 10

# 设置请求间隔（避免被限流）
python update_manager.py update --sleep 2.0

# 试运行模式（不实际执行）
python update_manager.py update --dry-run
```

### 环境变量控制

```bash
# 限制处理的股票数量
export LIMIT_SYMBOLS=50
export LIMIT_NEW_STOCKS=10

# 设置请求间隔时间
export SLEEP_SECONDS=1.5

# 然后运行程序
python update_manager.py update
```

## 程序特点

### 1. 智能增量更新
- 自动检测每只股票的最新数据日期
- 只下载缺失的新数据，避免重复
- 大幅提高更新效率

### 2. 新股票自动发现
- 对比市场股票列表和数据库现有股票
- 自动识别需要添加的新股票
- 下载完整历史数据

### 3. 错误处理和日志
- 完善的异常处理机制
- 详细的操作日志记录
- 更新状态跟踪

### 4. 灵活的控制选项
- 支持限制处理数量（测试用）
- 可调节请求频率（避免限流）
- 试运行模式

## 数据库表结构

程序会自动创建以下表：

### daily_data 表（主数据表）
```sql
CREATE TABLE daily_data (
    date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    amount DOUBLE,
    outstanding_share DOUBLE,
    turnover DOUBLE,
    symbol VARCHAR,
    name VARCHAR,
    adjust VARCHAR
);
```

### update_log 表（更新日志）
```sql
CREATE TABLE update_log (
    symbol VARCHAR,
    last_date_before DATE,
    last_date_after DATE,
    new_rows INTEGER,
    update_time TIMESTAMP,
    status VARCHAR,
    error_message VARCHAR
);
```

## 使用建议

### 1. 首次使用
如果是全新的数据库，建议先运行：
```bash
python update_manager.py add-new
```

### 2. 日常更新
建议每天运行增量更新：
```bash
python update_manager.py update
```

### 3. 定期全量更新
建议每周运行一次全量更新：
```bash
python update_manager.py full
```

### 4. 测试新功能
使用限制参数进行小规模测试：
```bash
python update_manager.py update --limit 5 --dry-run
```

## 注意事项

1. **请求频率**: 建议设置适当的请求间隔，避免被数据源限流
2. **网络稳定**: 确保网络连接稳定，程序会自动重试失败的请求
3. **磁盘空间**: 确保有足够的临时存储空间
4. **数据质量**: 程序会自动验证数据格式，跳过无效数据

## 依赖包

确保安装以下Python包：
```bash
pip install akshare pandas duckdb
```