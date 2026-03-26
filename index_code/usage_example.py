#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数数据使用示例
展示如何在实际项目中使用指数数据
"""

import duckdb
import pandas as pd
from datetime import datetime, timedelta

def connect_db(db_path="/Users/yuping/Downloads/ossdata/duckdb/index_market.duckdb"):
    """连接数据库"""
    return duckdb.connect(db_path)

def example_1_basic_query():
    """示例1: 基础数据查询"""
    print("📊 示例1: 基础数据查询")
    print("-" * 40)
    
    con = connect_db()
    
    # 获取上证指数最近10天数据
    query = """
    SELECT date, open, high, low, close, volume
    FROM index_daily_data 
    WHERE symbol = 'sh000001'
    ORDER BY date DESC 
    LIMIT 10
    """
    
    df = con.execute(query).df()
    print("上证指数最近10天数据:")
    print(df.to_string(index=False))
    
    con.close()

def example_2_technical_analysis():
    """示例2: 技术分析计算"""
    print("\n📈 示例2: 技术分析 - 移动平均线")
    print("-" * 40)
    
    con = connect_db()
    
    # 计算移动平均线
    query = """
    SELECT 
        date,
        close,
        AVG(close) OVER (ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as MA5,
        AVG(close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as MA20
    FROM index_daily_data 
    WHERE symbol = 'sh000001'
    ORDER BY date DESC 
    LIMIT 10
    """
    
    df = con.execute(query).df()
    df['MA5'] = df['MA5'].round(2)
    df['MA20'] = df['MA20'].round(2)
    
    print("上证指数移动平均线:")
    print(df.to_string(index=False))
    
    con.close()

def example_3_performance_comparison():
    """示例3: 指数表现对比"""
    print("\n🏆 示例3: 主要指数表现对比 (近30天)")
    print("-" * 40)
    
    con = connect_db()
    
    query = """
    WITH recent_data AS (
        SELECT 
            symbol,
            index_name,
            date,
            close,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
        FROM index_daily_data 
        WHERE symbol IN ('sh000001', 'sz399001', 'sh000016', 'sz399006')
    ),
    performance AS (
        SELECT 
            symbol,
            index_name,
            MAX(CASE WHEN rn = 1 THEN close END) as latest_close,
            MAX(CASE WHEN rn = 30 THEN close END) as month_ago_close
        FROM recent_data
        WHERE rn <= 30
        GROUP BY symbol, index_name
    )
    SELECT 
        symbol,
        index_name,
        latest_close,
        month_ago_close,
        ROUND((latest_close - month_ago_close) / month_ago_close * 100, 2) as return_30d
    FROM performance
    WHERE month_ago_close IS NOT NULL
    ORDER BY return_30d DESC
    """
    
    df = con.execute(query).df()
    print("主要指数30天表现:")
    print(df.to_string(index=False))
    
    con.close()

def example_4_volatility_analysis():
    """示例4: 波动率分析"""
    print("\n📊 示例4: 波动率分析")
    print("-" * 40)
    
    con = connect_db()
    
    query = """
    WITH daily_returns AS (
        SELECT 
            symbol,
            index_name,
            date,
            close,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
            (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100 as daily_return
        FROM index_daily_data 
        WHERE symbol IN ('sh000001', 'sz399001', 'sh000016')
        AND date >= (SELECT MAX(date) - INTERVAL '60 days' FROM index_daily_data)
    )
    SELECT 
        symbol,
        index_name,
        COUNT(*) as trading_days,
        ROUND(AVG(daily_return), 4) as avg_daily_return,
        ROUND(STDDEV(daily_return), 4) as daily_volatility,
        ROUND(STDDEV(daily_return) * SQRT(252), 2) as annualized_volatility
    FROM daily_returns
    WHERE daily_return IS NOT NULL
    GROUP BY symbol, index_name
    ORDER BY annualized_volatility DESC
    """
    
    df = con.execute(query).df()
    print("主要指数波动率分析 (近60天):")
    print(df.to_string(index=False))
    
    con.close()

def example_5_export_data():
    """示例5: 数据导出"""
    print("\n💾 示例5: 数据导出")
    print("-" * 40)
    
    con = connect_db()
    
    # 导出上证指数近一年数据
    query = """
    SELECT 
        date,
        open,
        high,
        low,
        close,
        volume
    FROM index_daily_data 
    WHERE symbol = 'sh000001'
    AND date >= (SELECT MAX(date) - INTERVAL '1 year' FROM index_daily_data)
    ORDER BY date
    """
    
    df = con.execute(query).df()
    
    # 保存为CSV
    filename = f"sh000001_1year_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(filename, index=False)
    
    print(f"✅ 已导出上证指数近一年数据到: {filename}")
    print(f"📊 数据条数: {len(df)}")
    print(f"📅 日期范围: {df['date'].min()} ~ {df['date'].max()}")
    
    con.close()

def example_6_custom_analysis():
    """示例6: 自定义分析 - 寻找最佳表现指数"""
    print("\n🔍 示例6: 自定义分析 - 近期最佳表现指数")
    print("-" * 40)
    
    con = connect_db()
    
    query = """
    WITH recent_performance AS (
        SELECT 
            symbol,
            index_name,
            exchange,
            FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date DESC ROWS BETWEEN 29 FOLLOWING AND 29 FOLLOWING) as start_close,
            FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date DESC) as latest_close,
            COUNT(*) OVER (PARTITION BY symbol) as data_points
        FROM index_daily_data 
        WHERE date >= (SELECT MAX(date) - INTERVAL '30 days' FROM index_daily_data)
    )
    SELECT DISTINCT
        symbol,
        index_name,
        exchange,
        ROUND((latest_close - start_close) / start_close * 100, 2) as return_30d,
        ROUND(latest_close, 2) as latest_close
    FROM recent_performance
    WHERE data_points >= 20  -- 至少20个交易日
    AND start_close IS NOT NULL 
    AND latest_close IS NOT NULL
    ORDER BY return_30d DESC
    LIMIT 10
    """
    
    df = con.execute(query).df()
    print("近30天表现最佳的10个指数:")
    print(df.to_string(index=False))
    
    con.close()

def main():
    """运行所有示例"""
    print("🚀 指数数据使用示例")
    print("=" * 60)
    
    try:
        example_1_basic_query()
        example_2_technical_analysis()
        example_3_performance_comparison()
        example_4_volatility_analysis()
        example_5_export_data()
        example_6_custom_analysis()
        
        print("\n" + "=" * 60)
        print("✅ 所有示例运行完成!")
        print("💡 你可以基于这些示例开发自己的分析程序")
        print("📚 更多SQL查询示例请参考: index_code/sql_examples.sql")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ 运行示例时发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()