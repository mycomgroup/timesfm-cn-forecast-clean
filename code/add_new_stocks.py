# -*- coding: utf-8 -*-
import os
import sys
import duckdb
import akshare as ak
import pandas as pd
import time
import logging
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

class NewStockAdder:
    def __init__(self, db_path, temp_dir="/tmp/new_stocks"):
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
        
    def get_existing_symbols(self):
        """获取数据库中已存在的股票代码"""
        try:
            result = self.con.execute("SELECT DISTINCT symbol FROM daily_data").fetchall()
            return set(row[0] for row in result)
        except:
            return set()
            
    def get_all_market_symbols(self):
        """获取市场上所有股票代码"""
        logger.info("获取A股市场所有股票列表...")
        
        try:
            # 获取A股列表
            stock_list_df = ak.stock_info_a_code_name()
            
            # 添加前缀
            def add_prefix(code):
                code = str(code).zfill(6)
                if code.startswith("6"):
                    return "sh" + code
                elif code.startswith(("0", "3")):
                    return "sz" + code
                elif code.startswith(("4", "8")):
                    return "bj" + code
                else:
                    return code
                    
            stock_list_df["full_symbol"] = stock_list_df["code"].apply(add_prefix)
            
            return stock_list_df[["full_symbol", "name"]].values.tolist()
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
            
    def find_new_symbols(self):
        """找出需要添加的新股票"""
        existing_symbols = self.get_existing_symbols()
        all_symbols = self.get_all_market_symbols()
        
        new_symbols = []
        for symbol, name in all_symbols:
            if symbol not in existing_symbols:
                new_symbols.append((symbol, name))
                
        logger.info(f"发现 {len(new_symbols)} 只新股票需要添加")
        return new_symbols
        
    def download_full_history(self, symbol, name, adjust="qfq"):
        """下载股票的完整历史数据"""
        try:
            logger.info(f"下载 {symbol}({name}) 的完整历史数据...")
            
            # 从1991年开始下载
            start_date = "19910403"
            end_date = datetime.now().strftime("%Y%m%d")
            
            df = ak.stock_zh_a_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            if df.empty:
                logger.warning(f"没有数据: {symbol}")
                return None
                
            # 标准化列名
            if "date" in df.columns:
                df.rename(columns={"date": "日期"}, inplace=True)
                
            # 添加元数据
            df["symbol"] = symbol
            df["name"] = name
            df["adjust"] = adjust
            
            # 保存到临时文件
            temp_file = os.path.join(self.temp_dir, f"{symbol}.csv")
            df.to_csv(temp_file, index=False)
            
            logger.info(f"下载完成 {symbol}: {len(df)} 行数据")
            return temp_file, len(df)
            
        except Exception as e:
            logger.error(f"下载失败 {symbol}: {e}")
            return None
            
    def import_csv_to_db(self, csv_file, symbol):
        """将CSV文件导入到数据库"""
        try:
            # 使用DuckDB直接读取CSV
            self.con.execute(f"""
                INSERT INTO daily_data 
                SELECT 
                    CAST(日期 AS DATE) as date,
                    CAST(开盘 AS DOUBLE) as open,
                    CAST(最高 AS DOUBLE) as high,
                    CAST(最低 AS DOUBLE) as low,
                    CAST(收盘 AS DOUBLE) as close,
                    CAST(成交量 AS DOUBLE) as volume,
                    CAST(成交额 AS DOUBLE) as amount,
                    CAST(COALESCE(流通股本, 0) AS DOUBLE) as outstanding_share,
                    CAST(COALESCE(换手率, 0) AS DOUBLE) as turnover,
                    symbol,
                    name,
                    adjust
                FROM read_csv_auto('{csv_file}')
                WHERE 日期 IS NOT NULL 
                AND 日期 != ''
                AND 日期 ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
            """)
            
            # 获取插入的行数
            count = self.con.execute(f"SELECT COUNT(*) FROM daily_data WHERE symbol = '{symbol}'").fetchone()[0]
            logger.info(f"成功导入 {symbol}: {count} 行数据")
            
            return count
            
        except Exception as e:
            logger.error(f"导入失败 {symbol}: {e}")
            return 0
            
    def add_new_symbol(self, symbol, name):
        """添加单个新股票"""
        try:
            # 下载数据
            result = self.download_full_history(symbol, name)
            if not result:
                return False
                
            temp_file, expected_rows = result
            
            # 导入到数据库
            imported_rows = self.import_csv_to_db(temp_file, symbol)
            
            # 清理临时文件
            if os.path.exists(temp_file):
                os.remove(temp_file)
                
            return imported_rows > 0
            
        except Exception as e:
            logger.error(f"添加新股票失败 {symbol}: {e}")
            return False
            
    def add_all_new_stocks(self, limit=None, sleep_seconds=2):
        """添加所有新股票"""
        new_symbols = self.find_new_symbols()
        
        if not new_symbols:
            logger.info("没有发现新股票")
            return
            
        if limit:
            new_symbols = new_symbols[:limit]
            logger.info(f"限制添加数量: {limit}")
            
        success_count = 0
        failed_count = 0
        
        for i, (symbol, name) in enumerate(new_symbols, 1):
            logger.info(f"进度 {i}/{len(new_symbols)}: 添加 {symbol}({name})")
            
            if self.add_new_symbol(symbol, name):
                success_count += 1
            else:
                failed_count += 1
                
            # 休眠避免请求过于频繁
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
                
        logger.info(f"添加完成! 成功: {success_count}, 失败: {failed_count}")
        
    def close(self):
        """关闭数据库连接"""
        if self.con:
            self.con.close()


def main():
    # 配置参数
    db_path = "/Users/yuping/Downloads/ossdata/duckdb/market.duckdb"
    temp_dir = "/tmp/new_stocks"
    
    # 从环境变量获取限制数量
    limit = None
    if "LIMIT_NEW_STOCKS" in os.environ:
        try:
            limit = int(os.environ["LIMIT_NEW_STOCKS"])
        except:
            limit = None
            
    # 从环境变量获取休眠时间
    sleep_seconds = 2
    if "SLEEP_SECONDS" in os.environ:
        try:
            sleep_seconds = float(os.environ["SLEEP_SECONDS"])
        except:
            sleep_seconds = 2
    
    adder = NewStockAdder(db_path, temp_dir)
    
    try:
        adder.connect()
        adder.add_all_new_stocks(limit=limit, sleep_seconds=sleep_seconds)
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        adder.close()


if __name__ == "__main__":
    main()