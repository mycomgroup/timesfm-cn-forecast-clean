#!/usr/bin/env python3
import duckdb

def check_database():
    db_path = "/Users/yuping/Downloads/ossdata/duckdb/market.duckdb"
    con = duckdb.connect(db_path)
    
    # 检查数据库中的股票数量
    total_symbols = con.execute("SELECT COUNT(DISTINCT symbol) FROM daily_data").fetchone()[0]
    print(f"数据库中的股票数量: {total_symbols}")
    
    # 显示前10个股票及其最新日期
    print("\n前10个股票:")
    stocks = con.execute("""
        SELECT DISTINCT symbol, name, MAX(date) as last_date 
        FROM daily_data 
        GROUP BY symbol, name 
        ORDER BY symbol 
        LIMIT 10
    """).fetchall()
    
    for symbol, name, last_date in stocks:
        print(f"{symbol} - {name} - {last_date}")
    
    # 检查不同交易所的股票分布
    print("\n交易所分布:")
    exchanges = con.execute("""
        SELECT 
            CASE 
                WHEN symbol LIKE 'sh%' THEN '上交所'
                WHEN symbol LIKE 'sz%' THEN '深交所'
                WHEN symbol LIKE 'bj%' THEN '北交所'
                ELSE '其他'
            END as exchange,
            COUNT(DISTINCT symbol) as count
        FROM daily_data 
        GROUP BY 1
        ORDER BY count DESC
    """).fetchall()
    
    for exchange, count in exchanges:
        print(f"{exchange}: {count}只")
    
    con.close()

if __name__ == "__main__":
    check_database()