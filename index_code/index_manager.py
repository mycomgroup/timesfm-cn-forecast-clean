#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票指数数据管理器
统一的命令行界面，支持多种操作模式
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
    parser = argparse.ArgumentParser(description='股票指数数据管理器')
    parser.add_argument('mode', choices=['import', 'update', 'check'], 
                       help='操作模式: import=导入历史数据, update=增量更新, check=检查数据库状态')
    parser.add_argument('--db-path', default='/Users/yuping/Downloads/ossdata/duckdb/index_market.duckdb',
                       help='DuckDB数据库路径')
    parser.add_argument('--source-dir', default='/Users/yuping/Downloads/ossdata/stock_zh_index_daily',
                       help='源数据目录路径')
    parser.add_argument('--limit', type=int, help='限制处理的数量')
    parser.add_argument('--sleep', type=float, default=1.0, help='请求间隔时间(秒)')
    parser.add_argument('--dry-run', action='store_true', help='试运行模式，不实际执行')
    
    args = parser.parse_args()
    
    logger.info(f"开始执行 {args.mode} 模式")
    logger.info(f"数据库路径: {args.db_path}")
    logger.info(f"限制数量: {args.limit or '无限制'}")
    
    if args.dry_run:
        logger.info("试运行模式，不会实际执行操作")
        return 0
    
    start_time = datetime.now()
    
    try:
        if args.mode == 'import':
            # 导入历史数据
            logger.info("执行历史数据导入...")
            
            # 检查源目录
            if not os.path.exists(args.source_dir):
                logger.error(f"源数据目录不存在: {args.source_dir}")
                return 1
            
            # 设置环境变量
            if args.limit:
                os.environ["LIMIT_FILES"] = str(args.limit)
            
            # 导入index_ingest模块并执行
            sys.path.insert(0, os.path.dirname(__file__))
            from index_ingest import main as import_main
            
            # 临时修改数据库路径
            import index_ingest
            index_ingest.main.__globals__['base_dir'] = args.source_dir
            index_ingest.main.__globals__['db_path'] = args.db_path
            
            result = import_main()
            return result
            
        elif args.mode == 'update':
            # 增量更新
            logger.info("执行增量更新...")
            
            # 检查数据库文件是否存在
            if not os.path.exists(args.db_path):
                logger.error(f"数据库文件不存在: {args.db_path}")
                logger.info("请先运行 import 模式导入历史数据")
                return 1
            
            # 设置环境变量
            if args.limit:
                os.environ["LIMIT_INDICES"] = str(args.limit)
            os.environ["SLEEP_SECONDS"] = str(args.sleep)
            
            # 导入index_update模块并执行
            sys.path.insert(0, os.path.dirname(__file__))
            from index_update import IndexUpdater
            
            updater = IndexUpdater(args.db_path)
            updater.connect()
            updater.update_all(limit=args.limit, sleep_seconds=args.sleep)
            updater.close()
            
        elif args.mode == 'check':
            # 检查数据库状态
            logger.info("检查数据库状态...")
            
            if not os.path.exists(args.db_path):
                logger.error(f"数据库文件不存在: {args.db_path}")
                return 1
            
            # 导入check_index_db模块并执行
            sys.path.insert(0, os.path.dirname(__file__))
            from check_index_db import check_database
            
            check_database(args.db_path)
            
    except KeyboardInterrupt:
        logger.info("用户中断执行")
        return 1
    except Exception as e:
        logger.error(f"执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    elapsed = datetime.now() - start_time
    logger.info(f"执行完成，耗时: {elapsed}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())