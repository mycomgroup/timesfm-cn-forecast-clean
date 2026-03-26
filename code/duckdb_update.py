# -*- coding: utf-8 -*-
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
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

class DuckDBUpdater:
    def __init__(self, db_path, temp_dir="/tmp/update_data"):
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
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS daily_data (
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
            )
        """)
        
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS update_log (
                symbol VARCHAR,
                last_date_before DATE,
                last_date_after DATE,
                new_rows INTEGER,
                update_time TIMESTAMP,
                status VARCHAR,
                error_message VARCHAR
            )
        """)
        
    def get_symbols_to_update(self):
        """获取需要更新的股票列表"""
        # 从数据库中获取所有已有的股票代码，优先选择上交所和深交所
        existing_symbols = self.con.execute("""
            SELECT DISTINCT symbol, name, MAX(date) as last_date 
            FROM daily_data 
            GROUP BY symbol, name
            ORDER BY 
                CASE 
                    WHEN symbol LIKE 'sh%' THEN 1
                    WHEN symbol LIKE 'sz%' THEN 2
                    WHEN symbol LIKE 'bj%' THEN 3
                    ELSE 4
                END,
                symbol
        """).fetchall()
        
        return existing_symbols
        
    def get_last_date_for_symbol(self, symbol):
        """获取指定股票的最新日期"""
        result = self.con.execute(
            "SELECT MAX(date) FROM daily_data WHERE symbol = ?", 
            [symbol]
        ).fetchone()
        return result[0] if result[0] else None
        
    def download_incremental_data(self, symbol, name, start_date, end_date, adjust="qfq"):
        """下载增量数据"""
        try:
            logger.info(f"下载 {symbol}({name}) 从 {start_date} 到 {end_date}")
            
            # 转换日期格式为akshare需要的格式
            start_date_str = start_date.strftime("%Y%m%d")
            end_date_str = end_date.strftime("%Y%m%d")
            
            # 检查是否是北交所股票，如果是则跳过（akshare对北交所支持有限）
            if symbol.startswith('bj'):
                logger.warning(f"跳过北交所股票 {symbol}，akshare支持有限")
                return None
            
            # 使用akshare下载数据
            df = ak.stock_zh_a_daily(
                symbol=symbol, 
                start_date=start_date_str, 
                end_date=end_date_str, 
                adjust=adjust
            )
            
            if df.empty:
                logger.info(f"没有新数据: {symbol}")
                return None
                
            # 标准化列名
            if "date" in df.columns:
                df.rename(columns={"date": "日期"}, inplace=True)
                
            # 添加元数据
            df["symbol"] = symbol
            df["name"] = name
            df["adjust"] = adjust
            
            # 保存到临时文件
            temp_file = os.path.join(self.temp_dir, f"{symbol}_update.csv")
            df.to_csv(temp_file, index=False)
            
            return temp_file, len(df)
            
        except Exception as e:
            logger.error(f"下载数据失败 {symbol}: {e}")
            return None
            
    def parse_csv_and_insert(self, csv_file, symbol):
        """解析CSV文件并插入到数据库"""
        try:
            rows_inserted = 0
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows_to_insert = []
                
                for row in reader:
                    # 跳过标题行或无效行
                    if not self.is_valid_data_row(row):
                        continue
                        
                    # 转换数据格式
                    data_tuple = self.convert_row_to_tuple(row)
                    if data_tuple:
                        rows_to_insert.append(data_tuple)
                        
                if rows_to_insert:
                    # 批量插入
                    self.con.executemany(
                        "INSERT INTO daily_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", 
                        rows_to_insert
                    )
                    rows_inserted = len(rows_to_insert)
                    
            return rows_inserted
            
        except Exception as e:
            logger.error(f"插入数据失败 {symbol}: {e}")
            return 0
            
    def is_valid_data_row(self, row):
        """验证数据行是否有效"""
        try:
            # 检查必要字段
            date_field = row.get("日期") or row.get("date")
            if not date_field or not re.match(r"\d{4}-\d{2}-\d{2}", str(date_field)):
                return False
                
            # 检查数值字段 (兼容英文和中文列名)
            numeric_candidates = [
                "open", "high", "low", "close", "volume", "amount", "outstanding_share", "turnover",
                "开盘", "最高", "最低", "收盘", "成交量", "成交额", "流通股本", "换手率"
            ]
            for field in numeric_candidates:
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
            
    def convert_row_to_tuple(self, row):
        """将CSV行转换为数据库元组"""
        try:
            date_val = row.get("日期") or row.get("date")
            open_val = float(row.get("open") or row.get("开盘", 0) or 0)
            high_val = float(row.get("high") or row.get("最高", 0) or 0)
            low_val = float(row.get("low") or row.get("最低", 0) or 0)
            close_val = float(row.get("close") or row.get("收盘", 0) or 0)
            volume_val = float(row.get("volume") or row.get("成交量", 0) or 0)
            amount_val = float(row.get("amount") or row.get("成交额", 0) or 0)
            outstanding_val = float(row.get("outstanding_share") or row.get("流通股本", 0) or 0)
            turnover_val = float(row.get("turnover") or row.get("换手率", 0) or 0)
            symbol_val = row.get("symbol", "")
            name_val = row.get("name", "")
            adjust_val = row.get("adjust", "qfq")
            
            return (
                date_val, open_val, high_val, low_val, close_val,
                volume_val, amount_val, outstanding_val, turnover_val,
                symbol_val, name_val, adjust_val
            )
        except Exception as e:
            logger.error(f"转换数据行失败: {e}")
            return None
            
    def update_symbol(self, symbol, name, last_date_in_db):
        """更新单个股票的数据"""
        try:
            # 计算需要更新的日期范围
            if last_date_in_db:
                start_date = datetime.strptime(str(last_date_in_db), "%Y-%m-%d") + timedelta(days=1)
            else:
                start_date = datetime(1991, 4, 3)  # 默认开始日期
                
            end_date = datetime.now()
            
            # 如果开始日期已经是今天或之后，跳过
            if start_date.date() >= end_date.date():
                logger.info(f"跳过 {symbol}: 数据已是最新")
                return 0
                
            # 下载增量数据
            result = self.download_incremental_data(symbol, name, start_date, end_date)
            if not result:
                return 0
                
            temp_file, expected_rows = result
            
            # 插入数据到数据库
            inserted_rows = self.parse_csv_and_insert(temp_file, symbol)
            
            # 获取更新后的最新日期
            new_last_date = self.get_last_date_for_symbol(symbol)
            
            # 记录更新日志
            self.con.execute("""
                INSERT INTO update_log 
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
                INSERT INTO update_log 
                (symbol, last_date_before, last_date_after, new_rows, update_time, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                symbol, last_date_in_db, None, 0,
                datetime.now(), "failed", error_msg
            ])
            return 0
            
    def update_all(self, limit=None, sleep_seconds=1):
        """更新所有股票数据"""
        logger.info("开始更新所有股票数据...")
        
        symbols_to_update = self.get_symbols_to_update()
        total_symbols = len(symbols_to_update)
        
        if limit:
            symbols_to_update = symbols_to_update[:limit]
            logger.info(f"限制更新数量: {limit}")
            
        logger.info(f"需要检查更新的股票数量: {len(symbols_to_update)}")
        
        total_updated_rows = 0
        success_count = 0
        failed_count = 0
        
        for i, (symbol, name, last_date) in enumerate(symbols_to_update, 1):
            logger.info(f"进度 {i}/{len(symbols_to_update)}: 处理 {symbol}")
            
            updated_rows = self.update_symbol(symbol, name, last_date)
            
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
    db_path = "/Users/yuping/Downloads/git/timesfm-cn-forecast-clean/data/market.duckdb"
    temp_dir = "/tmp/duckdb_update"
    
    # 从环境变量获取限制数量
    limit = None
    if "LIMIT_SYMBOLS" in os.environ:
        try:
            limit = int(os.environ["LIMIT_SYMBOLS"])
        except:
            limit = None
            
    # 从环境变量获取休眠时间
    sleep_seconds = 1
    if "SLEEP_SECONDS" in os.environ:
        try:
            sleep_seconds = float(os.environ["SLEEP_SECONDS"])
        except:
            sleep_seconds = 1
    
    updater = DuckDBUpdater(db_path, temp_dir)
    
    try:
        updater.connect()
        updater.update_all(limit=limit, sleep_seconds=sleep_seconds)
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        updater.close()


if __name__ == "__main__":
    main()