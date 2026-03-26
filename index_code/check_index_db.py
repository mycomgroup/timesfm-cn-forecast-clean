#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数数据库状态检查程序
"""

import duckdb
import sys

def check_database(db_path=None):
    """检查指数数据库状态"""
    if db_path is None:
        db_path = "/Users/yuping/Downloads/ossdata/duckdb/index_market.duckdb"
    
    try:
        con = duckdb.connect(db_path)
        
        print("=" * 60)
        print("指数数据库状态报告")
        print("=" * 60)
        
        # 检查表是否存在
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"\n数据库表: {len(tables)}个")
        for table in tables:
            print(f"  - {table[0]}")
        
        # 检查主数据表
        if any('index_daily_data' in str(table) for table in tables):
            # 总体统计
            total_indices = con.execute("SELECT COUNT(DISTINCT symbol) FROM index_daily_data").fetchone()[0]
            total_records = con.execute("SELECT COUNT(*) FROM index_daily_data").fetchone()[0]
            
            print(f"\n总体统计:")
            print(f"  指数数量: {total_indices}")
            print(f"  数据记录: {total_records:,}")
            
            # 日期范围
            date_range = con.execute("SELECT MIN(date), MAX(date) FROM index_daily_data").fetchone()
            print(f"  日期范围: {date_range[0]} ~ {date_range[1]}")
            
            # 交易所分布
            print(f"\n交易所分布:")
            exchanges = con.execute("""
                SELECT exchange, COUNT(DISTINCT symbol) as count
                FROM index_daily_data 
                GROUP BY exchange 
                ORDER BY count DESC
            """).fetchall()
            
            for exchange, count in exchanges:
                print(f"  {exchange}: {count}个指数")
            
            # 分类分布
            print(f"\n指数分类:")
            categories = con.execute("""
                SELECT category, COUNT(DISTINCT symbol) as count
                FROM index_daily_data 
                GROUP BY category 
                ORDER BY count DESC
            """).fetchall()
            
            for category, count in categories:
                print(f"  {category}: {count}个指数")
            
            # 主要指数最新数据
            print(f"\n主要指数最新数据:")
            major_indices = con.execute("""
                SELECT symbol, index_name, MAX(date) as last_date, COUNT(*) as records
                FROM index_daily_data 
                WHERE symbol IN ('sh000001', 'sz399001', 'sh000016', 'sz399006', 'sh000300')
                GROUP BY symbol, index_name
                ORDER BY symbol
            """).fetchall()
            
            for symbol, name, last_date, records in major_indices:
                print(f"  {symbol} - {name}: {last_date} ({records:,}条记录)")
            
            # 数据量最大的前10个指数
            print(f"\n数据量最大的前10个指数:")
            top_indices = con.execute("""
                SELECT symbol, index_name, MIN(date) as first_date, MAX(date) as last_date, COUNT(*) as records
                FROM index_daily_data 
                GROUP BY symbol, index_name
                ORDER BY records DESC
                LIMIT 10
            """).fetchall()
            
            for symbol, name, first_date, last_date, records in top_indices:
                print(f"  {symbol} - {name}: {records:,}条记录 ({first_date} ~ {last_date})")
        
        # 检查更新日志
        if any('index_update_log' in str(table) for table in tables):
            print(f"\n最近更新记录:")
            recent_updates = con.execute("""
                SELECT symbol, last_date_before, last_date_after, new_rows, status, update_time
                FROM index_update_log 
                ORDER BY update_time DESC 
                LIMIT 10
            """).fetchall()
            
            if recent_updates:
                for symbol, before, after, new_rows, status, update_time in recent_updates:
                    print(f"  {symbol}: {before} -> {after} (+{new_rows}行) [{status}] {update_time}")
            else:
                print("  暂无更新记录")
        
        # 检查导入日志
        if any('index_import_log' in str(table) for table in tables):
            print(f"\n导入统计:")
            import_stats = con.execute("""
                SELECT 
                    COUNT(*) as total_files,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_files,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_files,
                    SUM(imported_rows) as total_imported
                FROM index_import_log
            """).fetchone()
            
            if import_stats and import_stats[0] > 0:
                total, success, failed, imported = import_stats
                print(f"  处理文件: {total}个")
                print(f"  成功文件: {success}个")
                print(f"  失败文件: {failed}个")
                print(f"  导入记录: {imported:,}条")
            else:
                print("  暂无导入记录")
        
        print("=" * 60)
        
    except Exception as e:
        print(f"检查数据库失败: {e}")
        return 1
    finally:
        if 'con' in locals():
            con.close()
    
    return 0

def main():
    """主函数"""
    db_path = None
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    return check_database(db_path)

if __name__ == "__main__":
    sys.exit(main())