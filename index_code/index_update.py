#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票指数增量更新程序
基于duckdb_update.py改造，专门用于更新指数数据
"""

import os
import sys
import csv
import re
from datetime import datetime, timedelta
import duckdb
import akshare as ak
import pandas as pd
import time
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class IndexUpdater:
    def __init__(self, db_path, temp_dir="/tmp/index_update"):
        self.db_path = db_path
        self.temp_dir = temp_dir
        self.con = None
        self.ensure_temp_dir()
        
    def ensure_temp_dir(self):
        """确保临时目录存在"""
        os.makedirs(self.temp_dir, exist_ok=True)
        
    def connect(self):
        """连接到DuckDB"""
        self.con = duckdb.connect(self.db_path)
        self.ensure_schema()
        
    def ensure_schema(self):
        """确保数据库表结构存在"""
        # 主数据表
        self.con.execute("""
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
        
        # 更新日志表
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS index_update_log (
                symbol VARCHAR,
                last_date_before DATE,
                last_date_after DATE,
                new_rows INTEGER,
                update_time TIMESTAMP,
                status VARCHAR,
                error_message VARCHAR
            )
        """)
        
    def get_indices_to_update(self):
        """获取需要更新的指数列表"""
        # 从数据库中获取所有已有的指数代码，优先处理主要指数
        existing_indices = self.con.execute("""
            SELECT DISTINCT symbol, index_name, MAX(date) as last_date 
            FROM index_daily_data 
            GROUP BY symbol, index_name
            ORDER BY 
                CASE 
                    WHEN symbol IN ('sh000001', 'sz399001', 'sh000016', 'sz399006', 'sh000300') THEN 1
                    WHEN symbol LIKE 'sh000%' THEN 2
                    WHEN symbol LIKE 'sz399%' THEN 3
                    ELSE 4
                END,
                symbol
        """).fetchall()
        
        return existing_indices
        
    def get_last_date_for_index(self, symbol):
        """获取指定指数的最新日期"""
        result = self.con.execute(
            "SELECT MAX(date) FROM index_daily_data WHERE symbol = ?", 
            [symbol]
        ).fetchone()
        return result[0] if result[0] else None
        
    def download_incremental_data(self, symbol, start_date, end_date):
        """下载指数增量数据"""
        try:
            logger.info(f"下载指数 {symbol} 的完整数据并筛选增量部分")
            
            # akshare的指数接口只能获取全部数据，然后我们筛选需要的部分
            df = ak.stock_zh_index_daily(symbol=symbol)
            
            if df.empty:
                logger.info(f"没有数据: {symbol}")
                return None
            
            # 确保date列是datetime类型
            df['date'] = pd.to_datetime(df['date'])
            
            # 筛选需要的日期范围
            start_date_pd = pd.to_datetime(start_date)
            end_date_pd = pd.to_datetime(end_date)
            
            # 筛选增量数据
            mask = (df['date'] >= start_date_pd) & (df['date'] <= end_date_pd)
            df_filtered = df[mask].copy()
            
            if df_filtered.empty:
                logger.info(f"没有新数据: {symbol}")
                return None
            
            # 转换日期格式为字符串
            df_filtered['date'] = df_filtered['date'].dt.strftime('%Y-%m-%d')
                
            # 标准化列名
            df_filtered.rename(columns={
                'date': '日期',
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量'
            }, inplace=True)
            
            # 确保必要的列存在
            required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
            for col in required_columns:
                if col not in df_filtered.columns:
                    logger.warning(f"缺少列 {col}，使用默认值")
                    if col == "成交量":
                        df_filtered[col] = 0
                    else:
                        df_filtered[col] = 0.0
            
            # 添加symbol列
            df_filtered["symbol"] = symbol
            
            # 保存到临时文件
            temp_file = os.path.join(self.temp_dir, f"{symbol}_update.csv")
            df_filtered.to_csv(temp_file, index=False)
            
            logger.info(f"筛选出 {len(df_filtered)} 条新数据")
            return temp_file, len(df_filtered)
            
        except Exception as e:
            logger.error(f"下载指数数据失败 {symbol}: {e}")
            return None
            
    def parse_csv_and_insert(self, csv_file, symbol):
        """解析CSV文件并插入到数据库"""
        try:
            rows_inserted = 0
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows_to_insert = []
                
                for row in reader:
                    # 跳过无效行
                    if not self.is_valid_index_row(row):
                        continue
                        
                    # 转换数据格式
                    data_tuple = self.convert_row_to_tuple(row, symbol)
                    if data_tuple:
                        rows_to_insert.append(data_tuple)
                        
                if rows_to_insert:
                    # 批量插入
                    self.con.executemany(
                        "INSERT INTO index_daily_data VALUES (?,?,?,?,?,?,?,?,?,?)", 
                        rows_to_insert
                    )
                    rows_inserted = len(rows_to_insert)
                    
            return rows_inserted
            
        except Exception as e:
            logger.error(f"插入指数数据失败 {symbol}: {e}")
            return 0
            
    def is_valid_index_row(self, row):
        """验证指数数据行是否有效"""
        try:
            # 检查日期字段
            date_field = row.get("日期") or row.get("date")
            if not date_field or not re.match(r"\d{4}-\d{2}-\d{2}", str(date_field)):
                return False
                
            # 检查数值字段
            numeric_fields = ["开盘", "最高", "最低", "收盘"]
            for field in numeric_fields:
                value = row.get(field)
                if value is None or value == "":
                    continue
                try:
                    float(value)
                except:
                    return False
                    
            return True
        except:
            return False
            
    def convert_row_to_tuple(self, row, symbol):
        """将CSV行转换为数据库元组"""
        try:
            date_val = row.get("日期") or row.get("date")
            open_val = float(row.get("开盘", 0) or 0)
            high_val = float(row.get("最高", 0) or 0)
            low_val = float(row.get("最低", 0) or 0)
            close_val = float(row.get("收盘", 0) or 0)
            volume_val = float(row.get("成交量", 0) or 0)
            
            # 获取指数信息
            index_name = self.get_index_name(symbol)
            exchange = self.get_exchange(symbol)
            category = self.get_category(symbol)
            
            return (
                date_val, open_val, high_val, low_val, close_val,
                volume_val, symbol, index_name, exchange, category
            )
        except Exception as e:
            logger.error(f"转换数据行失败: {e}")
            return None
            
    def get_index_name(self, symbol):
        """根据指数代码获取指数名称"""
        # 先从数据库查询
        result = self.con.execute(
            "SELECT DISTINCT index_name FROM index_daily_data WHERE symbol = ? LIMIT 1", 
            [symbol]
        ).fetchone()
        
        if result and result[0]:
            return result[0]
        
        # 默认名称映射
        index_names = {
            'sh000001': '上证指数',
            'sh000002': 'A股指数',
            'sh000016': '上证50',
            'sh000300': '沪深300',
            'sz399001': '深证成指',
            'sz399006': '创业板指',
        }
        return index_names.get(symbol, f'指数{symbol}')
        
    def get_exchange(self, symbol):
        """根据指数代码获取交易所"""
        if symbol.startswith('sh'):
            return '上交所'
        elif symbol.startswith('sz'):
            return '深交所'
        else:
            return '其他'
            
    def get_category(self, symbol):
        """根据指数代码获取分类"""
        if symbol.startswith('sh000'):
            return '上证系列'
        elif symbol.startswith('sz399'):
            return '深证系列'
        else:
            return '其他'
            
    def update_index(self, symbol, index_name, last_date_in_db):
        """更新单个指数的数据"""
        try:
            # 计算需要更新的日期范围
            if last_date_in_db:
                start_date = datetime.strptime(str(last_date_in_db), "%Y-%m-%d") + timedelta(days=1)
            else:
                start_date = datetime(1990, 1, 1)  # 默认开始日期
                
            end_date = datetime.now()
            
            # 如果开始日期已经是今天或之后，跳过
            if start_date.date() >= end_date.date():
                logger.info(f"跳过 {symbol}: 数据已是最新")
                return 0
                
            # 下载增量数据
            result = self.download_incremental_data(symbol, start_date, end_date)
            if not result:
                return 0
                
            temp_file, expected_rows = result
            
            # 插入数据到数据库
            inserted_rows = self.parse_csv_and_insert(temp_file, symbol)
            
            # 获取更新后的最新日期
            new_last_date = self.get_last_date_for_index(symbol)
            
            # 记录更新日志
            self.con.execute("""
                INSERT INTO index_update_log 
                (symbol, last_date_before, last_date_after, new_rows, update_time, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                symbol, last_date_in_db, new_last_date, inserted_rows,
                datetime.now(), "success", None
            ])
            
            # 清理临时文件
            if os.path.exists(temp_file):
                os.remove(temp_file)
                
            logger.info(f"更新完成 {symbol}: 新增 {inserted_rows} 行数据")
            return inserted_rows
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"更新失败 {symbol}: {error_msg}")
            
            # 记录错误日志
            self.con.execute("""
                INSERT INTO index_update_log 
                (symbol, last_date_before, last_date_after, new_rows, update_time, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                symbol, last_date_in_db, None, 0,
                datetime.now(), "failed", error_msg
            ])
            return 0
            
    def update_all(self, limit=None, sleep_seconds=1):
        """更新所有指数数据"""
        logger.info("开始更新所有指数数据...")
        
        indices_to_update = self.get_indices_to_update()
        total_indices = len(indices_to_update)
        
        if limit:
            indices_to_update = indices_to_update[:limit]
            logger.info(f"限制更新数量: {limit}")
            
        logger.info(f"需要检查更新的指数数量: {len(indices_to_update)}")
        
        total_updated_rows = 0
        success_count = 0
        failed_count = 0
        
        for i, (symbol, index_name, last_date) in enumerate(indices_to_update, 1):
            logger.info(f"进度 {i}/{len(indices_to_update)}: 处理 {symbol}")
            
            updated_rows = self.update_index(symbol, index_name, last_date)
            
            if updated_rows > 0:
                success_count += 1
                total_updated_rows += updated_rows
            elif updated_rows == 0:
                # 可能是没有新数据，不算失败
                pass
            else:
                failed_count += 1
                
            # 休眠避免请求过于频繁
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
                
        logger.info(f"更新完成! 成功: {success_count}, 失败: {failed_count}, 总新增行数: {total_updated_rows}")
        
    def close(self):
        """关闭数据库连接"""
        if self.con:
            self.con.close()


def main():
    # 配置参数
    db_path = "/Users/yuping/Downloads/git/timesfm-cn-forecast-clean/data/index_market.duckdb"
    temp_dir = "/tmp/index_update"
    
    # 从环境变量获取限制数量
    limit = None
    if "LIMIT_INDICES" in os.environ:
        try:
            limit = int(os.environ["LIMIT_INDICES"])
        except:
            limit = None
            
    # 从环境变量获取休眠时间
    sleep_seconds = 1
    if "SLEEP_SECONDS" in os.environ:
        try:
            sleep_seconds = float(os.environ["SLEEP_SECONDS"])
        except:
            sleep_seconds = 1
    
    updater = IndexUpdater(db_path, temp_dir)
    
    try:
        updater.connect()
        updater.update_all(limit=limit, sleep_seconds=sleep_seconds)
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        updater.close()


if __name__ == "__main__":
    main()