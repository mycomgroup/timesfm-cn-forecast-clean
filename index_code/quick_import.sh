#!/bin/bash

# 股票指数数据快速全量导入脚本

echo "=========================================="
echo "股票指数数据全量导入"
echo "=========================================="

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "❌ 错误: 未找到python3"
    exit 1
fi

# 检查源数据目录
SOURCE_DIR="/Users/yuping/Downloads/ossdata/stock_zh_index_daily"
if [ ! -d "$SOURCE_DIR" ]; then
    echo "❌ 错误: 源数据目录不存在: $SOURCE_DIR"
    exit 1
fi

# 统计文件数量
FILE_COUNT=$(find "$SOURCE_DIR" -name "*.csv" | wc -l)
echo "📁 发现 $FILE_COUNT 个CSV文件"

# 估算时间
ESTIMATED_MIN=$((FILE_COUNT * 3 / 60))
echo "⏱️  预计耗时: $ESTIMATED_MIN - $((ESTIMATED_MIN * 2)) 分钟"

# 确认导入
echo ""
read -p "确认开始全量导入? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ 用户取消导入"
    exit 0
fi

echo ""
echo "🚀 开始全量导入..."
echo "开始时间: $(date)"
echo ""

# 执行导入
START_TIME=$(date +%s)

python3 index_code/index_manager.py import

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
ELAPSED_MIN=$((ELAPSED / 60))
ELAPSED_SEC=$((ELAPSED % 60))

echo ""
echo "=========================================="
echo "导入完成!"
echo "结束时间: $(date)"
echo "总耗时: ${ELAPSED_MIN}分${ELAPSED_SEC}秒"
echo "=========================================="

# 检查结果
echo ""
echo "📊 检查导入结果..."
python3 index_code/index_manager.py check

echo ""
echo "✅ 全量导入流程完成!"