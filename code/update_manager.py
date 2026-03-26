#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB股票数据更新管理器
支持多种更新模式：
1. 增量更新现有股票数据
2. 添加新股票
3. 全量更新（增量+新股票）
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='DuckDB股票数据更新管理器')
    parser.add_argument('mode', choices=['update', 'add-new', 'full'], 
                       help='更新模式: update=增量更新, add-new=添加新股票, full=全量更新')
    parser.add_argument('--db-path', default='/Users/yuping/Downloads/ossdata/duckdb/market.duckdb',
                       help='DuckDB数据库路径')
    parser.add_argument('--limit', type=int, help='限制处理的股票数量')
    parser.add_argument('--sleep', type=float, default=1.0, help='请求间隔时间(秒)')
    parser.add_argument('--dry-run', action='store_true', help='试运行模式，不实际执行')
    
    args = parser.parse_args()
    
    # 检查数据库文件是否存在
    if not os.path.exists(args.db_path):
        logger.error(f"数据库文件不存在: {args.db_path}")
        return 1
        
    # 设置环境变量
    if args.limit:
        os.environ["LIMIT_SYMBOLS"] = str(args.limit)
        os.environ["LIMIT_NEW_STOCKS"] = str(args.limit)
    os.environ["SLEEP_SECONDS"] = str(args.sleep)
    
    logger.info(f"开始执行 {args.mode} 模式")
    logger.info(f"数据库路径: {args.db_path}")
    logger.info(f"限制数量: {args.limit or '无限制'}")
    logger.info(f"请求间隔: {args.sleep}秒")
    
    if args.dry_run:
        logger.info("试运行模式，不会实际执行更新")
        return 0
    
    start_time = datetime.now()
    
    try:
        if args.mode == 'update':
            # 增量更新现有股票
            logger.info("执行增量更新...")
            from duckdb_update import DuckDBUpdater
            
            updater = DuckDBUpdater(args.db_path)
            updater.connect()
            updater.update_all(limit=args.limit, sleep_seconds=args.sleep)
            updater.close()
            
        elif args.mode == 'add-new':
            # 添加新股票
            logger.info("添加新股票...")
            from add_new_stocks import NewStockAdder
            
            adder = NewStockAdder(args.db_path)
            adder.connect()
            adder.add_all_new_stocks(limit=args.limit, sleep_seconds=args.sleep)
            adder.close()
            
        elif args.mode == 'full':
            # 全量更新：先添加新股票，再增量更新
            logger.info("执行全量更新...")
            
            # 1. 添加新股票
            logger.info("第一步: 添加新股票...")
            from add_new_stocks import NewStockAdder
            
            adder = NewStockAdder(args.db_path)
            adder.connect()
            adder.add_all_new_stocks(limit=args.limit, sleep_seconds=args.sleep)
            adder.close()
            
            # 2. 增量更新所有股票
            logger.info("第二步: 增量更新所有股票...")
            from duckdb_update import DuckDBUpdater
            
            updater = DuckDBUpdater(args.db_path)
            updater.connect()
            updater.update_all(limit=args.limit, sleep_seconds=args.sleep)
            updater.close()
            
    except KeyboardInterrupt:
        logger.info("用户中断执行")
        return 1
    except Exception as e:
        logger.error(f"执行失败: {e}")
        return 1
    
    elapsed = datetime.now() - start_time
    logger.info(f"执行完成，耗时: {elapsed}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())