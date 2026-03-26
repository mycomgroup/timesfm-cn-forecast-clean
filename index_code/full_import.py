#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票指数数据全量导入脚本
专门用于一次性导入所有历史数据
"""

import os
import sys
import time
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO, 
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'full_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

def main():
    """全量导入主函数"""
    
    # 配置参数
    source_dir = "/Users/yuping/Downloads/ossdata/stock_zh_index_daily"
    db_path = "/Users/yuping/Downloads/ossdata/duckdb/index_market.duckdb"
    
    logger.info("=" * 60)
    logger.info("股票指数数据全量导入开始")
    logger.info("=" * 60)
    logger.info(f"源数据目录: {source_dir}")
    logger.info(f"目标数据库: {db_path}")
    
    # 检查源目录
    if not os.path.exists(source_dir):
        logger.error(f"源数据目录不存在: {source_dir}")
        return 1
    
    # 统计文件数量
    csv_files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
    total_files = len(csv_files)
    logger.info(f"发现 {total_files} 个CSV文件待导入")
    
    # 确认导入
    print(f"\n即将导入 {total_files} 个指数文件到数据库")
    print(f"预计耗时: {total_files * 3 // 60} - {total_files * 5 // 60} 分钟")
    
    confirm = input("\n确认开始全量导入? (y/N): ").strip().lower()
    if confirm not in ['y', 'yes']:
        logger.info("用户取消导入")
        return 0
    
    # 记录开始时间
    start_time = datetime.now()
    logger.info(f"开始时间: {start_time}")
    
    try:
        # 导入index_ingest模块
        sys.path.insert(0, os.path.dirname(__file__))
        from index_ingest import main as import_main
        
        # 临时修改全局变量
        import index_ingest
        original_base_dir = index_ingest.main.__globals__.get('base_dir')
        original_db_path = index_ingest.main.__globals__.get('db_path')
        
        # 设置新的路径
        index_ingest.main.__globals__['base_dir'] = source_dir
        index_ingest.main.__globals__['db_path'] = db_path
        
        # 执行导入
        result = import_main()
        
        # 恢复原始值
        if original_base_dir:
            index_ingest.main.__globals__['base_dir'] = original_base_dir
        if original_db_path:
            index_ingest.main.__globals__['db_path'] = original_db_path
        
        # 计算耗时
        end_time = datetime.now()
        elapsed = end_time - start_time
        
        logger.info("=" * 60)
        if result == 0:
            logger.info("✅ 全量导入成功完成!")
        else:
            logger.error("❌ 全量导入失败!")
        
        logger.info(f"开始时间: {start_time}")
        logger.info(f"结束时间: {end_time}")
        logger.info(f"总耗时: {elapsed}")
        logger.info("=" * 60)
        
        # 生成最终报告
        generate_final_report(db_path)
        
        return result
        
    except KeyboardInterrupt:
        logger.warning("⚠️ 用户中断导入")
        return 1
    except Exception as e:
        logger.error(f"❌ 导入过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

def generate_final_report(db_path):
    """生成最终导入报告"""
    try:
        import duckdb
        
        logger.info("生成最终导入报告...")
        con = duckdb.connect(db_path)
        
        # 总体统计
        total_indices = con.execute("SELECT COUNT(DISTINCT symbol) FROM index_daily_data").fetchone()[0]
        total_records = con.execute("SELECT COUNT(*) FROM index_daily_data").fetchone()[0]
        date_range = con.execute("SELECT MIN(date), MAX(date) FROM index_daily_data").fetchone()
        
        logger.info("📊 最终统计报告:")
        logger.info(f"  📈 总指数数量: {total_indices}")
        logger.info(f"  📋 总数据记录: {total_records:,}")
        logger.info(f"  📅 数据日期范围: {date_range[0]} ~ {date_range[1]}")
        
        # 交易所分布
        exchanges = con.execute("""
            SELECT exchange, COUNT(DISTINCT symbol) as count
            FROM index_daily_data 
            GROUP BY exchange 
            ORDER BY count DESC
        """).fetchall()
        
        logger.info("🏢 交易所分布:")
        for exchange, count in exchanges:
            logger.info(f"  {exchange}: {count}个指数")
        
        # 导入成功率
        import_stats = con.execute("""
            SELECT 
                COUNT(*) as total_files,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_files,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_files
            FROM index_import_log
        """).fetchone()
        
        if import_stats:
            total, success, failed = import_stats
            success_rate = (success / total * 100) if total > 0 else 0
            logger.info("📈 导入成功率:")
            logger.info(f"  总文件: {total}")
            logger.info(f"  成功: {success} ({success_rate:.1f}%)")
            logger.info(f"  失败: {failed}")
        
        con.close()
        
    except Exception as e:
        logger.error(f"生成报告失败: {e}")

if __name__ == "__main__":
    sys.exit(main())