#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票指数历史数据批量导入程序
基于duckdb_ingest.py改造，专门用于处理指数数据
"""

import os
import sys
import csv
import re
from datetime import datetime
import duckdb
import traceback
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def parse_float(x):
    """解析浮点数"""
    try:
        return float(x) if x and x.strip() else 0.0
    except:
        return 0.0

def valid_index_row(row):
    """验证指数数据行是否有效"""
    if len(row) != 7:  # date,open,high,low,close,volume,symbol
        return False
    
    # 验证日期格式
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", row[0]):
        return False
    
    # 验证数值字段
    try:
        float(row[1])  # open
        float(row[2])  # high
        float(row[3])  # low
        float(row[4])  # close
        float(row[5])  # volume
    except:
        return False
    
    # 验证symbol字段
    if not row[6] or not row[6].strip():
        return False
        
    return True

def to_index_tuple(row):
    """将CSV行转换为数据库元组"""
    return (
        row[0],                    # date
        parse_float(row[1]),       # open
        parse_float(row[2]),       # high
        parse_float(row[3]),       # low
        parse_float(row[4]),       # close
        parse_float(row[5]),       # volume
        row[6].strip(),            # symbol
        get_index_name(row[6]),    # index_name
        get_exchange(row[6]),      # exchange
        get_category(row[6])       # category
    )

def get_index_name(symbol):
    """根据指数代码获取指数名称"""
    # 这里可以扩展为从配置文件或API获取
    index_names = {
        'sh000001': '上证指数',
        'sh000002': 'A股指数',
        'sh000003': 'B股指数',
        'sh000016': '上证50',
        'sh000300': '沪深300',
        'sz399001': '深证成指',
        'sz399002': '深成指R',
        'sz399003': '成份B指',
        'sz399006': '创业板指',
        'sz399300': '沪深300',
    }
    return index_names.get(symbol, f'指数{symbol}')

def get_exchange(symbol):
    """根据指数代码获取交易所"""
    if symbol.startswith('sh'):
        return '上交所'
    elif symbol.startswith('sz'):
        return '深交所'
    else:
        return '其他'

def get_category(symbol):
    """根据指数代码获取分类"""
    if symbol.startswith('sh000'):
        return '上证系列'
    elif symbol.startswith('sz399'):
        return '深证系列'
    elif symbol.startswith('sz970'):
        return '深证特殊'
    elif symbol.startswith('sz980'):
        return '深证特殊'
    else:
        return '其他'

def ensure_index_schema(con):
    """确保数据库表结构存在"""
    # 主数据表
    con.execute("""
        CREATE TABLE IF NOT EXISTS index_daily_data (
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
        )
    """)
    
    # 指数信息表
    con.execute("""
        CREATE TABLE IF NOT EXISTS index_info (
            symbol VARCHAR PRIMARY KEY,
            index_name VARCHAR,
            exchange VARCHAR,
            category VARCHAR,
            first_date DATE,
            last_date DATE,
            total_records INTEGER,
            created_time TIMESTAMP,
            updated_time TIMESTAMP
        )
    """)
    
    # 导入日志表
    con.execute("""
        CREATE TABLE IF NOT EXISTS index_import_log (
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
        )
    """)
    
    logger.info("数据库表结构检查完成")

def process_index_file(con, file_path):
    """处理单个指数文件"""
    rows_total = 0
    rows_valid = []
    rows_invalid = 0
    invalid_samples = []
    first_date = None
    last_date = None
    symbol_value = None
    status = "success"
    error_msg = None
    
    try:
        logger.info(f"开始处理文件: {os.path.basename(file_path)}")
        
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            
            if header is None or len(header) != 7:
                raise ValueError("文件头格式错误")
            
            for row in reader:
                rows_total += 1
                
                if not valid_index_row(row):
                    rows_invalid += 1
                    if len(invalid_samples) < 3:
                        invalid_samples.append(",".join(row))
                    continue
                
                t = to_index_tuple(row)
                rows_valid.append(t)
                
                # 记录日期范围和symbol
                date_val = t[0]
                if first_date is None or date_val < first_date:
                    first_date = date_val
                if last_date is None or date_val > last_date:
                    last_date = date_val
                symbol_value = t[6]
        
        # 批量插入数据
        imported_rows = 0
        if rows_valid:
            con.executemany(
                "INSERT INTO index_daily_data VALUES (?,?,?,?,?,?,?,?,?,?)", 
                rows_valid
            )
            imported_rows = len(rows_valid)
            
            # 更新或插入指数信息
            update_index_info(con, symbol_value, first_date, last_date, imported_rows)
            
    except Exception as e:
        status = "failed"
        error_msg = str(e)
        imported_rows = 0
        logger.error(f"处理文件失败 {file_path}: {error_msg}")
    
    # 记录导入日志
    con.execute("""
        INSERT INTO index_import_log 
        (symbol, file_path, rows_total, rows_valid, rows_invalid, 
         first_date, last_date, imported_rows, import_time, status, error_message)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, [
        symbol_value or "",
        file_path,
        rows_total,
        len(rows_valid),
        rows_invalid,
        first_date,
        last_date,
        imported_rows,
        datetime.now(),
        status,
        error_msg,
    ])
    
    # 检查数据库中的最新日期
    last_in_db = None
    if symbol_value:
        result = con.execute(
            "SELECT MAX(date) FROM index_daily_data WHERE symbol = ?", 
            [symbol_value]
        ).fetchone()
        last_in_db = result[0] if result else None
    
    # 输出处理结果
    logger.info(f"文件处理完成: {os.path.basename(file_path)}")
    logger.info(f"  指数代码: {symbol_value}")
    logger.info(f"  状态: {status}")
    logger.info(f"  总行数: {rows_total}")
    logger.info(f"  有效行数: {len(rows_valid)}")
    logger.info(f"  无效行数: {rows_invalid}")
    logger.info(f"  文件日期范围: {first_date} ~ {last_date}")
    logger.info(f"  数据库最新日期: {last_in_db}")
    
    if invalid_samples:
        logger.warning("无效数据样本:")
        for sample in invalid_samples:
            logger.warning(f"  {sample}")
    
    return imported_rows, status == "success"

def update_index_info(con, symbol, first_date, last_date, new_records):
    """更新指数信息表"""
    try:
        # 检查指数是否已存在
        existing = con.execute(
            "SELECT symbol FROM index_info WHERE symbol = ?", 
            [symbol]
        ).fetchone()
        
        if existing:
            # 更新现有记录
            con.execute("""
                UPDATE index_info 
                SET last_date = ?, 
                    total_records = total_records + ?, 
                    updated_time = ?
                WHERE symbol = ?
            """, [last_date, new_records, datetime.now(), symbol])
        else:
            # 插入新记录
            con.execute("""
                INSERT INTO index_info 
                (symbol, index_name, exchange, category, first_date, last_date, 
                 total_records, created_time, updated_time)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, [
                symbol,
                get_index_name(symbol),
                get_exchange(symbol),
                get_category(symbol),
                first_date,
                last_date,
                new_records,
                datetime.now(),
                datetime.now()
            ])
            
    except Exception as e:
        logger.error(f"更新指数信息失败 {symbol}: {e}")

def main():
    """主函数"""
    # 配置参数
    base_dir = "/Users/yuping/Downloads/ossdata/stock_zh_index_daily"
    db_path = "/Users/yuping/Downloads/ossdata/duckdb/index_market.duckdb"
    
    # 检查源目录
    if not os.path.exists(base_dir):
        logger.error(f"源数据目录不存在: {base_dir}")
        return 1
    
    # 连接数据库
    logger.info(f"连接数据库: {db_path}")
    con = duckdb.connect(db_path)
    
    try:
        # 确保表结构
        ensure_index_schema(con)
        
        # 获取所有CSV文件
        files = [
            os.path.join(base_dir, f) 
            for f in os.listdir(base_dir) 
            if f.endswith(".csv")
        ]
        files.sort()
        
        logger.info(f"找到 {len(files)} 个CSV文件")
        
        # 检查是否有限制
        limit = None
        if "LIMIT_FILES" in os.environ:
            try:
                limit = int(os.environ["LIMIT_FILES"])
                logger.info(f"限制处理文件数量: {limit}")
            except:
                pass
        
        # 处理文件
        count = 0
        total_imported = 0
        success_files = 0
        failed_files = 0
        start_time = datetime.now()
        
        for file_path in files:
            if limit is not None and count >= limit:
                break
            
            imported_rows, success = process_index_file(con, file_path)
            
            total_imported += imported_rows
            if success:
                success_files += 1
            else:
                failed_files += 1
            
            count += 1
        
        # 输出统计信息
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 50)
        logger.info("批量导入完成!")
        logger.info(f"处理文件数: {count}")
        logger.info(f"成功文件数: {success_files}")
        logger.info(f"失败文件数: {failed_files}")
        logger.info(f"总导入行数: {total_imported}")
        logger.info(f"耗时: {int(elapsed)}秒")
        
        # 生成指数汇总报告
        generate_summary_report(con)
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        traceback.print_exc()
        return 1
    finally:
        con.close()
    
    return 0

def generate_summary_report(con):
    """生成指数汇总报告"""
    logger.info("=" * 50)
    logger.info("指数数据汇总报告:")
    
    # 总体统计
    total_indices = con.execute("SELECT COUNT(DISTINCT symbol) FROM index_daily_data").fetchone()[0]
    total_records = con.execute("SELECT COUNT(*) FROM index_daily_data").fetchone()[0]
    logger.info(f"总指数数量: {total_indices}")
    logger.info(f"总数据记录: {total_records}")
    
    # 交易所分布
    logger.info("\n交易所分布:")
    exchanges = con.execute("""
        SELECT exchange, COUNT(DISTINCT symbol) as count
        FROM index_daily_data 
        GROUP BY exchange 
        ORDER BY count DESC
    """).fetchall()
    
    for exchange, count in exchanges:
        logger.info(f"  {exchange}: {count}个指数")
    
    # 前20个指数的数据量
    logger.info("\n前20个指数数据量:")
    top_indices = con.execute("""
        SELECT symbol, index_name, MIN(date) as first_date, MAX(date) as last_date, COUNT(*) as records
        FROM index_daily_data 
        GROUP BY symbol, index_name
        ORDER BY records DESC
        LIMIT 20
    """).fetchall()
    
    for symbol, name, first_date, last_date, records in top_indices:
        logger.info(f"  {symbol} - {name}: {records}条记录 ({first_date} ~ {last_date})")

if __name__ == "__main__":
    sys.exit(main())