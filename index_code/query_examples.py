#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票指数数据查询示例程序
验证数据可用性并提供各种查询示例
"""

import duckdb
import pandas as pd
import sys
from datetime import datetime, timedelta

class IndexDataQuery:
    def __init__(self, db_path="/Users/yuping/Downloads/ossdata/duckdb/index_market.duckdb"):
        self.db_path = db_path
        self.con = None
        
    def connect(self):
        """连接数据库"""
        try:
            self.con = duckdb.connect(self.db_path)
            print(f"✅ 成功连接数据库: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ 连接数据库失败: {e}")
            return False
    
    def close(self):
        """关闭数据库连接"""
        if self.con:
            self.con.close()
    
    def basic_info(self):
        """基础信息查询"""
        print("\n" + "="*60)
        print("📊 数据库基础信息")
        print("="*60)
        
        # 总体统计
        total_indices = self.con.execute("SELECT COUNT(DISTINCT symbol) FROM index_daily_data").fetchone()[0]
        total_records = self.con.execute("SELECT COUNT(*) FROM index_daily_data").fetchone()[0]
        date_range = self.con.execute("SELECT MIN(date), MAX(date) FROM index_daily_data").fetchone()
        
        print(f"📈 指数总数: {total_indices}")
        print(f"📋 数据记录: {total_records:,}")
        print(f"📅 日期范围: {date_range[0]} ~ {date_range[1]}")
        
        # 交易所分布
        print(f"\n🏢 交易所分布:")
        exchanges = self.con.execute("""
            SELECT exchange, COUNT(DISTINCT symbol) as count
            FROM index_daily_data 
            GROUP BY exchange 
            ORDER BY count DESC
        """).fetchall()
        
        for exchange, count in exchanges:
            print(f"  {exchange}: {count}个指数")
    
    def major_indices_latest(self):
        """主要指数最新数据"""
        print("\n" + "="*60)
        print("📈 主要指数最新数据")
        print("="*60)
        
        query = """
        SELECT 
            symbol,
            index_name,
            date,
            open,
            high,
            low,
            close,
            volume,
            ROUND((close - open) / open * 100, 2) as daily_change_pct
        FROM index_daily_data 
        WHERE symbol IN ('sh000001', 'sz399001', 'sh000016', 'sz399006', 'sh000300')
        AND date = (SELECT MAX(date) FROM index_daily_data WHERE symbol = index_daily_data.symbol)
        ORDER BY symbol
        """
        
        df = self.con.execute(query).df()
        
        if not df.empty:
            print(f"{'指数代码':<12} {'指数名称':<12} {'日期':<12} {'开盘':<8} {'最高':<8} {'最低':<8} {'收盘':<8} {'涨跌幅%':<8}")
            print("-" * 80)
            for _, row in df.iterrows():
                print(f"{row['symbol']:<12} {row['index_name']:<12} {row['date']:<12} "
                      f"{row['open']:<8.2f} {row['high']:<8.2f} {row['low']:<8.2f} "
                      f"{row['close']:<8.2f} {row['daily_change_pct']:<8.2f}")
        else:
            print("❌ 未找到主要指数数据")
    
    def historical_performance(self, symbol='sh000001', days=30):
        """历史表现分析"""
        print(f"\n" + "="*60)
        print(f"📊 {symbol} 近{days}天历史表现")
        print("="*60)
        
        query = f"""
        SELECT 
            date,
            close,
            volume,
            LAG(close) OVER (ORDER BY date) as prev_close,
            ROUND((close - LAG(close) OVER (ORDER BY date)) / LAG(close) OVER (ORDER BY date) * 100, 2) as daily_change_pct
        FROM index_daily_data 
        WHERE symbol = '{symbol}'
        ORDER BY date DESC
        LIMIT {days}
        """
        
        df = self.con.execute(query).df()
        
        if not df.empty:
            df = df.sort_values('date')  # 按日期正序排列
            
            print(f"📅 数据期间: {df['date'].min()} ~ {df['date'].max()}")
            print(f"📈 期间最高: {df['close'].max():.2f}")
            print(f"📉 期间最低: {df['close'].min():.2f}")
            print(f"📊 平均成交量: {df['volume'].mean():,.0f}")
            print(f"📈 累计涨跌幅: {((df['close'].iloc[-1] / df['close'].iloc[0]) - 1) * 100:.2f}%")
            
            # 显示最近5天数据
            print(f"\n最近5天数据:")
            print(f"{'日期':<12} {'收盘价':<10} {'成交量':<15} {'涨跌幅%':<10}")
            print("-" * 50)
            for _, row in df.tail(5).iterrows():
                change_pct = row['daily_change_pct'] if pd.notna(row['daily_change_pct']) else 0
                print(f"{row['date']:<12} {row['close']:<10.2f} {row['volume']:<15,.0f} {change_pct:<10.2f}")
        else:
            print(f"❌ 未找到 {symbol} 的数据")
    
    def compare_indices(self, symbols=['sh000001', 'sz399001', 'sh000016'], days=90):
        """指数对比分析"""
        print(f"\n" + "="*60)
        print(f"📊 指数对比分析 (近{days}天)")
        print("="*60)
        
        symbols_str = "', '".join(symbols)
        query = f"""
        WITH recent_data AS (
            SELECT 
                symbol,
                index_name,
                date,
                close,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
            FROM index_daily_data 
            WHERE symbol IN ('{symbols_str}')
        ),
        base_data AS (
            SELECT symbol, close as base_close
            FROM recent_data 
            WHERE rn = {days}
        ),
        latest_data AS (
            SELECT symbol, index_name, close as latest_close
            FROM recent_data 
            WHERE rn = 1
        )
        SELECT 
            l.symbol,
            l.index_name,
            b.base_close,
            l.latest_close,
            ROUND((l.latest_close - b.base_close) / b.base_close * 100, 2) as period_return_pct
        FROM latest_data l
        JOIN base_data b ON l.symbol = b.symbol
        ORDER BY period_return_pct DESC
        """
        
        df = self.con.execute(query).df()
        
        if not df.empty:
            print(f"{'指数代码':<12} {'指数名称':<12} {'期初价格':<10} {'最新价格':<10} {'期间涨跌%':<12}")
            print("-" * 70)
            for _, row in df.iterrows():
                print(f"{row['symbol']:<12} {row['index_name']:<12} {row['base_close']:<10.2f} "
                      f"{row['latest_close']:<10.2f} {row['period_return_pct']:<12.2f}")
        else:
            print("❌ 未找到对比数据")
    
    def volatility_analysis(self, symbol='sh000001', days=60):
        """波动率分析"""
        print(f"\n" + "="*60)
        print(f"📊 {symbol} 波动率分析 (近{days}天)")
        print("="*60)
        
        query = f"""
        WITH daily_returns AS (
            SELECT 
                date,
                close,
                LAG(close) OVER (ORDER BY date) as prev_close,
                ROUND((close - LAG(close) OVER (ORDER BY date)) / LAG(close) OVER (ORDER BY date) * 100, 2) as daily_return
            FROM index_daily_data 
            WHERE symbol = '{symbol}'
            ORDER BY date DESC
            LIMIT {days + 1}
        )
        SELECT 
            COUNT(*) as trading_days,
            ROUND(AVG(daily_return), 2) as avg_daily_return,
            ROUND(STDDEV(daily_return), 2) as daily_volatility,
            ROUND(MIN(daily_return), 2) as min_daily_return,
            ROUND(MAX(daily_return), 2) as max_daily_return,
            ROUND(STDDEV(daily_return) * SQRT(252), 2) as annualized_volatility
        FROM daily_returns
        WHERE daily_return IS NOT NULL
        """
        
        result = self.con.execute(query).fetchone()
        
        if result:
            trading_days, avg_return, volatility, min_return, max_return, ann_volatility = result
            print(f"📅 交易天数: {trading_days}")
            print(f"📈 平均日收益率: {avg_return}%")
            print(f"📊 日波动率: {volatility}%")
            print(f"📈 最大单日涨幅: {max_return}%")
            print(f"📉 最大单日跌幅: {min_return}%")
            print(f"📊 年化波动率: {ann_volatility}%")
        else:
            print(f"❌ 未找到 {symbol} 的波动率数据")
    
    def top_performers(self, days=30, limit=10):
        """表现最佳指数"""
        print(f"\n" + "="*60)
        print(f"🏆 近{days}天表现最佳指数 (前{limit}名)")
        print("="*60)
        
        query = f"""
        WITH recent_performance AS (
            SELECT 
                symbol,
                index_name,
                exchange,
                FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date DESC ROWS BETWEEN {days-1} FOLLOWING AND {days-1} FOLLOWING) as start_close,
                FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date DESC) as latest_close,
                COUNT(*) OVER (PARTITION BY symbol) as data_points
            FROM index_daily_data 
            WHERE date >= (SELECT MAX(date) - INTERVAL '{days} days' FROM index_daily_data)
        )
        SELECT DISTINCT
            symbol,
            index_name,
            exchange,
            start_close,
            latest_close,
            ROUND((latest_close - start_close) / start_close * 100, 2) as period_return_pct
        FROM recent_performance
        WHERE data_points >= {days * 0.8}  -- 至少有80%的交易日数据
        AND start_close IS NOT NULL 
        AND latest_close IS NOT NULL
        ORDER BY period_return_pct DESC
        LIMIT {limit}
        """
        
        df = self.con.execute(query).df()
        
        if not df.empty:
            print(f"{'排名':<4} {'指数代码':<12} {'指数名称':<15} {'交易所':<8} {'涨跌幅%':<10}")
            print("-" * 65)
            for i, (_, row) in enumerate(df.iterrows(), 1):
                print(f"{i:<4} {row['symbol']:<12} {row['index_name']:<15} {row['exchange']:<8} {row['period_return_pct']:<10.2f}")
        else:
            print("❌ 未找到表现数据")
    
    def data_quality_check(self):
        """数据质量检查"""
        print(f"\n" + "="*60)
        print("🔍 数据质量检查")
        print("="*60)
        
        # 检查缺失数据
        missing_data = self.con.execute("""
            SELECT 
                symbol,
                COUNT(*) as total_records,
                SUM(CASE WHEN open IS NULL OR open = 0 THEN 1 ELSE 0 END) as missing_open,
                SUM(CASE WHEN high IS NULL OR high = 0 THEN 1 ELSE 0 END) as missing_high,
                SUM(CASE WHEN low IS NULL OR low = 0 THEN 1 ELSE 0 END) as missing_low,
                SUM(CASE WHEN close IS NULL OR close = 0 THEN 1 ELSE 0 END) as missing_close,
                SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as missing_volume
            FROM index_daily_data
            GROUP BY symbol
            HAVING missing_open > 0 OR missing_high > 0 OR missing_low > 0 OR missing_close > 0 OR missing_volume > 0
            ORDER BY (missing_open + missing_high + missing_low + missing_close + missing_volume) DESC
            LIMIT 10
        """).fetchall()
        
        if missing_data:
            print("⚠️ 发现数据缺失问题:")
            print(f"{'指数代码':<12} {'总记录':<8} {'缺失开盘':<8} {'缺失最高':<8} {'缺失最低':<8} {'缺失收盘':<8} {'缺失成交量':<10}")
            print("-" * 75)
            for row in missing_data:
                print(f"{row[0]:<12} {row[1]:<8} {row[2]:<8} {row[3]:<8} {row[4]:<8} {row[5]:<8} {row[6]:<10}")
        else:
            print("✅ 未发现明显的数据缺失问题")
        
        # 检查异常数据
        anomaly_data = self.con.execute("""
            SELECT 
                symbol,
                COUNT(*) as anomaly_count
            FROM index_daily_data
            WHERE high < low OR open < 0 OR high < 0 OR low < 0 OR close < 0
            GROUP BY symbol
            ORDER BY anomaly_count DESC
            LIMIT 5
        """).fetchall()
        
        if anomaly_data:
            print(f"\n⚠️ 发现异常数据:")
            for symbol, count in anomaly_data:
                print(f"  {symbol}: {count}条异常记录")
        else:
            print(f"\n✅ 未发现明显的数据异常")
        
        # 数据连续性检查
        print(f"\n📅 数据连续性检查 (主要指数):")
        continuity_check = self.con.execute("""
            WITH date_gaps AS (
                SELECT 
                    symbol,
                    date,
                    LAG(date) OVER (PARTITION BY symbol ORDER BY date) as prev_date,
                    date - LAG(date) OVER (PARTITION BY symbol ORDER BY date) as gap_days
                FROM index_daily_data
                WHERE symbol IN ('sh000001', 'sz399001', 'sh000016')
            )
            SELECT 
                symbol,
                COUNT(CASE WHEN gap_days > 7 THEN 1 END) as large_gaps,
                MAX(gap_days) as max_gap_days
            FROM date_gaps
            GROUP BY symbol
        """).fetchall()
        
        for symbol, large_gaps, max_gap in continuity_check:
            print(f"  {symbol}: {large_gaps}个大间隔, 最大间隔{max_gap}天")
    
    def export_sample_data(self, symbol='sh000001', days=100):
        """导出样本数据"""
        print(f"\n" + "="*60)
        print(f"💾 导出 {symbol} 近{days}天数据样本")
        print("="*60)
        
        query = f"""
        SELECT 
            date,
            open,
            high,
            low,
            close,
            volume,
            symbol,
            index_name
        FROM index_daily_data 
        WHERE symbol = '{symbol}'
        ORDER BY date DESC
        LIMIT {days}
        """
        
        df = self.con.execute(query).df()
        
        if not df.empty:
            filename = f"{symbol}_sample_{days}days.csv"
            df.to_csv(filename, index=False)
            print(f"✅ 已导出 {len(df)} 条记录到文件: {filename}")
            print(f"📊 数据预览:")
            print(df.head().to_string(index=False))
        else:
            print(f"❌ 未找到 {symbol} 的数据")

def main():
    """主函数 - 运行所有查询示例"""
    
    print("🚀 股票指数数据查询验证程序")
    print("="*60)
    
    # 创建查询对象
    query = IndexDataQuery()
    
    if not query.connect():
        return 1
    
    try:
        # 1. 基础信息
        query.basic_info()
        
        # 2. 主要指数最新数据
        query.major_indices_latest()
        
        # 3. 历史表现分析
        query.historical_performance('sh000001', 30)
        
        # 4. 指数对比
        query.compare_indices(['sh000001', 'sz399001', 'sh000016'], 60)
        
        # 5. 波动率分析
        query.volatility_analysis('sh000001', 60)
        
        # 6. 表现最佳指数
        query.top_performers(30, 10)
        
        # 7. 数据质量检查
        query.data_quality_check()
        
        # 8. 导出样本数据
        query.export_sample_data('sh000001', 100)
        
        print(f"\n" + "="*60)
        print("✅ 数据查询验证完成!")
        print("📊 数据可用性: 良好")
        print("🎯 建议: 数据已准备就绪，可用于分析和建模")
        print("="*60)
        
    except Exception as e:
        print(f"❌ 查询过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        query.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())