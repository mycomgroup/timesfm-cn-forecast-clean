# 股票指数数据管理系统设计文档

## 1. 项目概述

### 1.1 目标
基于现有的股票数据更新系统，创建一个专门用于管理股票指数数据的系统，包括：
- 历史指数数据的批量导入
- 指数数据的增量更新
- 新指数的自动发现和添加
- 数据质量监控和日志记录

### 1.2 数据源
- **历史数据**: `/Users/yuping/Downloads/ossdata/stock_zh_index_daily/` 目录下的CSV文件
- **实时数据**: 通过akshare的`stock_zh_index_daily`接口获取
- **指数列表**: 通过akshare的`stock_zh_index_spot`接口获取

## 2. 数据结构分析

### 2.1 现有数据格式
```csv
date,open,high,low,close,volume,symbol
1990-12-19,96.05,99.98,95.79,99.98,126000,sh000001
```

### 2.2 字段说明
- `date`: 交易日期 (YYYY-MM-DD)
- `open`: 开盘点数
- `high`: 最高点数  
- `low`: 最低点数
- `close`: 收盘点数
- `volume`: 成交量
- `symbol`: 指数代码 (如: sh000001, sz399001)

### 2.3 指数分类
- **上交所指数**: sh开头 (如: sh000001-上证指数)
- **深交所指数**: sz开头 (如: sz399001-深证成指)
- **特殊指数**: sz970xxx, sz980xxx等

## 3. 系统架构

### 3.1 目录结构
```
index_code/
├── DESIGN.md                    # 设计文档
├── index_ingest.py             # 历史数据批量导入
├── index_update.py             # 增量更新程序
├── add_new_indices.py          # 新指数添加程序
├── index_manager.py            # 主控制程序
├── check_index_db.py           # 数据库状态检查
└── README_index.md             # 使用说明
```

### 3.2 数据库设计

#### 3.2.1 主数据表 (index_daily_data)
```sql
CREATE TABLE index_daily_data (
    date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    symbol VARCHAR,
    index_name VARCHAR,
    exchange VARCHAR,
    category VARCHAR
);
```

#### 3.2.2 指数信息表 (index_info)
```sql
CREATE TABLE index_info (
    symbol VARCHAR PRIMARY KEY,
    index_name VARCHAR,
    exchange VARCHAR,
    category VARCHAR,
    first_date DATE,
    last_date DATE,
    total_records INTEGER,
    created_time TIMESTAMP,
    updated_time TIMESTAMP
);
```

#### 3.2.3 导入日志表 (index_import_log)
```sql
CREATE TABLE index_import_log (
    symbol VARCHAR,
    file_path VARCHAR,
    rows_total INTEGER,
    rows_valid INTEGER,
    rows_invalid INTEGER,
    first_date DATE,
    last_date DATE,
    imported_rows INTEGER,
    import_time TIMESTAMP,
    status VARCHAR,
    error_message VARCHAR
);
```

#### 3.2.4 更新日志表 (index_update_log)
```sql
CREATE TABLE index_update_log (
    symbol VARCHAR,
    last_date_before DATE,
    last_date_after DATE,
    new_rows INTEGER,
    update_time TIMESTAMP,
    status VARCHAR,
    error_message VARCHAR
);
```

## 4. 核心功能模块

### 4.1 历史数据导入模块 (index_ingest.py)
- **功能**: 批量导入`stock_zh_index_daily`目录下的所有CSV文件
- **特点**: 
  - 数据验证和清洗
  - 重复数据检测
  - 错误处理和日志记录
  - 进度跟踪

### 4.2 增量更新模块 (index_update.py)
- **功能**: 检查现有指数的最新日期，下载缺失数据
- **特点**:
  - 智能日期检测
  - 只下载增量数据
  - 支持批量更新
  - 错误重试机制

### 4.3 新指数发现模块 (add_new_indices.py)
- **功能**: 自动发现市场新指数并添加历史数据
- **特点**:
  - 对比现有指数列表
  - 自动下载完整历史数据
  - 指数信息自动填充

### 4.4 主控制模块 (index_manager.py)
- **功能**: 统一的命令行界面
- **支持模式**:
  - `import`: 批量导入历史数据
  - `update`: 增量更新现有指数
  - `add-new`: 添加新指数
  - `full`: 全量更新 (新指数 + 增量更新)

## 5. 数据处理流程

### 5.1 初始化流程
1. 创建数据库和表结构
2. 批量导入历史CSV文件
3. 建立指数信息索引
4. 生成数据质量报告

### 5.2 日常更新流程
1. 检查现有指数最新日期
2. 计算需要更新的日期范围
3. 调用akshare接口下载数据
4. 数据验证和入库
5. 更新指数信息表
6. 记录更新日志

### 5.3 新指数处理流程
1. 获取市场所有指数列表
2. 对比数据库现有指数
3. 识别新增指数
4. 下载新指数完整历史数据
5. 更新指数信息表

## 6. 数据质量保证

### 6.1 数据验证规则
- 日期格式验证 (YYYY-MM-DD)
- 数值字段非空验证
- 价格数据合理性检查
- 重复数据检测

### 6.2 错误处理策略
- 网络请求重试机制
- 数据格式异常跳过
- 详细错误日志记录
- 失败任务重新调度

### 6.3 监控指标
- 数据更新成功率
- 数据完整性检查
- 更新延迟监控
- 异常指数识别

## 7. 性能优化

### 7.1 批量处理
- 批量SQL插入操作
- 事务管理优化
- 内存使用控制

### 7.2 并发控制
- 请求频率限制
- 连接池管理
- 资源锁定机制

### 7.3 存储优化
- 索引策略优化
- 数据压缩存储
- 历史数据归档

## 8. 扩展性设计

### 8.1 多数据源支持
- 预留其他数据源接口
- 数据源优先级配置
- 数据源切换机制

### 8.2 指数类型扩展
- 支持更多指数类型
- 自定义指数计算
- 指数分类管理

### 8.3 API接口
- RESTful API设计
- 数据查询接口
- 状态监控接口

## 9. 部署和运维

### 9.1 环境要求
- Python 3.9+
- DuckDB数据库
- akshare数据接口
- 足够的存储空间

### 9.2 配置管理
- 数据库连接配置
- API接口配置
- 日志级别配置
- 更新频率配置

### 9.3 监控告警
- 数据更新状态监控
- 异常情况告警
- 性能指标监控
- 存储空间监控

## 10. 使用场景

### 10.1 数据分析师
- 获取完整的指数历史数据
- 进行指数趋势分析
- 构建量化模型

### 10.2 投资研究
- 指数表现对比分析
- 市场趋势研究
- 投资策略回测

### 10.3 系统集成
- 为其他系统提供指数数据
- 数据仓库建设
- 实时数据流处理

## 11. 实施计划

### 阶段一: 基础功能开发 (1-2天)
- 数据库设计和创建
- 历史数据导入功能
- 基础数据验证

### 阶段二: 更新功能开发 (1天)
- 增量更新功能
- 新指数发现功能
- 主控制程序

### 阶段三: 优化和测试 (1天)
- 性能优化
- 错误处理完善
- 功能测试验证

### 阶段四: 文档和部署 (0.5天)
- 使用文档编写
- 部署脚本准备
- 运维手册编写