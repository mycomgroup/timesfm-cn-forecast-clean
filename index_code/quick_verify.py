#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速数据验证脚本
简单快速地验证指数数据是否可用
"""

import duckdb
import sys

def quick_verify(db_path="/Users/yuping/Downloads/ossdata/duckdb/index_market.duckdb"):
    """快速验证数据可用性"""
    
    print("🔍 快速数据验证...")
    
    try:
        con = duckdb.connect(db_path)
        
        # 1. 基础统计
        print("\n📊 基础统计:")
        total_indices = con.execute("SELECT COUNT(DISTINCT symbol) FROM index_daily_data").fetchone()[0]
        total_records = con.execute("SELECT COUNT(*) FROM index_daily_data").fetchone()[0]
        print(f"  指数数量: {total_indices}")
        print(f"  数据记录: {total_records:,}")
        
        # 2. 日期范围
        date_range = con.execute("SELECT MIN(date), MAX(date) FROM index_daily_data").fetchone()
        print(f"  日期范围: {date_range[0]} ~ {date_range[1]}")
        
        # 3. 主要指数检查
        print(f"\n📈 主要指数检查:")
        major_indices = ['sh000001', 'sz399001', 'sh000016', 'sz399006', 'sh000300']
        
        for symbol in major_indices:
            result = con.execute(f"""
                SELECT 
                    COUNT(*) as records,
                    MIN(date) as first_date,
                    MAX(date) as last_date,
                    MAX(close) as latest_close
                FROM index_daily_data 
                WHERE symbol = '{symbol}'
            """).fetchone()
            
            if result and result[0] > 0:
                records, first_date, last_date, latest_close = result
                print(f"  ✅ {symbol}: {records:,}条记录, 最新: {latest_close:.2f} ({last_date})")
            else:
                print(f"  ❌ {symbol}: 无数据")
        
        # 4. 数据完整性检查
        print(f"\n🔍 数据完整性:")
        null_check = con.execute("""
            SELECT 
                SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as null_close,
                SUM(CASE WHEN close <= 0 THEN 1 ELSE 0 END) as invalid_close
            FROM index_daily_data
        """).fetchone()
        
        null_close, invalid_close = null_check
        if null_close == 0 and invalid_close == 0:
            print(f"  ✅ 数据完整性良好")
        else:
            print(f"  ⚠️ 发现 {null_close} 个空值, {invalid_close} 个无效值")
        
        # 5. 最新数据检查
        print(f"\n📅 最新数据:")
        latest_date = con.execute("SELECT MAX(date) FROM index_daily_data").fetchone()[0]
        latest_count = con.execute(f"SELECT COUNT(DISTINCT symbol) FROM index_daily_data WHERE date = '{latest_date}'").fetchone()[0]
        print(f"  最新日期: {latest_date}")
        print(f"  当日指数数量: {latest_count}")
        
        # 6. 简单查询测试
        print(f"\n🧪 查询测试:")
        test_query = """
        SELECT 
            symbol,
            close,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close
        FROM index_daily_data 
        WHERE symbol = 'sh000001'
        ORDER BY date DESC 
        LIMIT 5
        """
        
        results = con.execute(test_query).fetchall()
        if results:
            print(f"  ✅ 复杂查询正常")
            print(f"  📊 上证指数最近5天收盘价:")
            for i, (symbol, close, prev_close) in enumerate(results):
                change = ((close - prev_close) / prev_close * 100) if prev_close else 0
                print(f"    {i+1}. {close:.2f} ({change:+.2f}%)")
        else:
            print(f"  ❌ 查询测试失败")
        
        con.close()
        
        # 总结
        print(f"\n" + "="*50)
        if total_indices > 0 and total_records > 0:
            print("✅ 数据验证通过!")
            print("🎯 数据可用于分析和查询")
            print(f"💡 建议运行完整验证: python3 index_code/query_examples.py")
        else:
            print("❌ 数据验证失败!")
            print("💡 请检查数据导入是否成功")
        print("="*50)
        
        return 0 if total_indices > 0 else 1
        
    except Exception as e:
        print(f"❌ 验证失败: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(quick_verify())