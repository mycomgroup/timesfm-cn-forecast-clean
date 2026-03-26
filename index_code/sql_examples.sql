-- 股票指数数据SQL查询示例
-- 可以直接在DuckDB中执行这些查询

-- ========================================
-- 1. 基础查询示例
-- ========================================

-- 查看数据库概况
SELECT 
    COUNT(DISTINCT symbol) as 指数数量,
    COUNT(*) as 总记录数,
    MIN(date) as 最早日期,
    MAX(date) as 最新日期
FROM index_daily_data;

-- 查看交易所分布
SELECT 
    exchange as 交易所,
    COUNT(DISTINCT symbol) as 指数数量,
    COUNT(*) as 数据记录数
FROM index_daily_data 
GROUP BY exchange 
ORDER BY 指数数量 DESC;

-- ========================================
-- 2. 主要指数查询
-- ========================================

-- 主要指数最新数据
SELECT 
    symbol as 指数代码,
    index_name as 指数名称,
    date as 日期,
    close as 收盘价,
    volume as 成交量,
    ROUND((close - open) / open * 100, 2) as 当日涨跌幅
FROM index_daily_data 
WHERE symbol IN ('sh000001', 'sz399001', 'sh000016', 'sz399006', 'sh000300')
AND date = (SELECT MAX(date) FROM index_daily_data WHERE symbol = index_daily_data.symbol)
ORDER BY symbol;

-- 上证指数历史最高最低点
SELECT 
    '历史最高' as 类型,
    date as 日期,
    high as 点位
FROM index_daily_data 
WHERE symbol = 'sh000001' 
AND high = (SELECT MAX(high) FROM index_daily_data WHERE symbol = 'sh000001')

UNION ALL

SELECT 
    '历史最低' as 类型,
    date as 日期,
    low as 点位
FROM index_daily_data 
WHERE symbol = 'sh000001' 
AND low = (SELECT MIN(low) FROM index_daily_data WHERE symbol = 'sh000001');

-- ========================================
-- 3. 技术分析查询
-- ========================================

-- 计算移动平均线 (以上证指数为例)
SELECT 
    date as 日期,
    close as 收盘价,
    ROUND(AVG(close) OVER (ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) as MA5,
    ROUND(AVG(close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 2) as MA20,
    ROUND(AVG(close) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW), 2) as MA60
FROM index_daily_data 
WHERE symbol = 'sh000001'
ORDER BY date DESC 
LIMIT 20;

-- 计算日收益率和累计收益率
WITH daily_returns AS (
    SELECT 
        date,
        close,
        LAG(close) OVER (ORDER BY date) as prev_close,
        ROUND((close - LAG(close) OVER (ORDER BY date)) / LAG(close) OVER (ORDER BY date) * 100, 4) as daily_return
    FROM index_daily_data 
    WHERE symbol = 'sh000001'
    ORDER BY date
)
SELECT 
    date as 日期,
    close as 收盘价,
    daily_return as 日收益率,
    ROUND(EXP(SUM(LN(1 + daily_return/100)) OVER (ORDER BY date)) - 1, 4) * 100 as 累计收益率
FROM daily_returns
WHERE daily_return IS NOT NULL
ORDER BY date DESC 
LIMIT 30;

-- ========================================
-- 4. 比较分析查询
-- ========================================

-- 多指数表现对比 (近一年)
WITH recent_year AS (
    SELECT 
        symbol,
        index_name,
        date,
        close,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
    FROM index_daily_data 
    WHERE symbol IN ('sh000001', 'sz399001', 'sh000016', 'sz399006')
    AND date >= (SELECT MAX(date) - INTERVAL '1 year' FROM index_daily_data)
),
performance AS (
    SELECT 
        symbol,
        index_name,
        MAX(CASE WHEN rn = 1 THEN close END) as latest_close,
        MAX(CASE WHEN rn = (SELECT MAX(rn) FROM recent_year r2 WHERE r2.symbol = recent_year.symbol) THEN close END) as year_ago_close
    FROM recent_year
    GROUP BY symbol, index_name
)
SELECT 
    symbol as 指数代码,
    index_name as 指数名称,
    year_ago_close as 一年前收盘价,
    latest_close as 最新收盘价,
    ROUND((latest_close - year_ago_close) / year_ago_close * 100, 2) as 年度涨跌幅
FROM performance
ORDER BY 年度涨跌幅 DESC;

-- 指数相关性分析 (近半年日收益率相关性)
WITH daily_returns AS (
    SELECT 
        date,
        symbol,
        ROUND((close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100, 4) as daily_return
    FROM index_daily_data 
    WHERE symbol IN ('sh000001', 'sz399001', 'sh000016')
    AND date >= (SELECT MAX(date) - INTERVAL '6 months' FROM index_daily_data)
),
pivot_returns AS (
    SELECT 
        date,
        MAX(CASE WHEN symbol = 'sh000001' THEN daily_return END) as sh000001_return,
        MAX(CASE WHEN symbol = 'sz399001' THEN daily_return END) as sz399001_return,
        MAX(CASE WHEN symbol = 'sh000016' THEN daily_return END) as sh000016_return
    FROM daily_returns
    WHERE daily_return IS NOT NULL
    GROUP BY date
    HAVING COUNT(*) = 3  -- 确保三个指数都有数据
)
SELECT 
    ROUND(CORR(sh000001_return, sz399001_return), 4) as 上证指数_深证成指_相关性,
    ROUND(CORR(sh000001_return, sh000016_return), 4) as 上证指数_上证50_相关性,
    ROUND(CORR(sz399001_return, sh000016_return), 4) as 深证成指_上证50_相关性
FROM pivot_returns;

-- ========================================
-- 5. 统计分析查询
-- ========================================

-- 各指数波动率排行 (近3个月)
WITH volatility_calc AS (
    SELECT 
        symbol,
        index_name,
        STDDEV(
            (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / 
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100
        ) as daily_volatility,
        COUNT(*) as trading_days
    FROM index_daily_data 
    WHERE date >= (SELECT MAX(date) - INTERVAL '3 months' FROM index_daily_data)
    GROUP BY symbol, index_name
    HAVING COUNT(*) >= 50  -- 至少50个交易日
)
SELECT 
    symbol as 指数代码,
    index_name as 指数名称,
    ROUND(daily_volatility, 4) as 日波动率,
    ROUND(daily_volatility * SQRT(252), 4) as 年化波动率,
    trading_days as 交易天数
FROM volatility_calc
ORDER BY 年化波动率 DESC
LIMIT 20;

-- 月度表现统计
SELECT 
    EXTRACT(YEAR FROM date) as 年份,
    EXTRACT(MONTH FROM date) as 月份,
    symbol as 指数代码,
    ROUND(
        (MAX(close) - MIN(close)) / MIN(close) * 100, 2
    ) as 月度涨跌幅,
    COUNT(*) as 交易天数
FROM index_daily_data 
WHERE symbol IN ('sh000001', 'sz399001')
AND date >= '2023-01-01'
GROUP BY EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date), symbol
ORDER BY 年份 DESC, 月份 DESC, symbol;

-- ========================================
-- 6. 高级分析查询
-- ========================================

-- 寻找历史上单日涨跌幅最大的情况
WITH daily_changes AS (
    SELECT 
        symbol,
        index_name,
        date,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
        ROUND((close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100, 2) as daily_change
    FROM index_daily_data
)
SELECT 
    '最大单日涨幅' as 类型,
    symbol as 指数代码,
    index_name as 指数名称,
    date as 日期,
    daily_change as 涨跌幅
FROM daily_changes
WHERE daily_change = (SELECT MAX(daily_change) FROM daily_changes WHERE daily_change IS NOT NULL)

UNION ALL

SELECT 
    '最大单日跌幅' as 类型,
    symbol as 指数代码,
    index_name as 指数名称,
    date as 日期,
    daily_change as 涨跌幅
FROM daily_changes
WHERE daily_change = (SELECT MIN(daily_change) FROM daily_changes WHERE daily_change IS NOT NULL);

-- 计算各指数的夏普比率 (近一年，假设无风险利率为3%)
WITH returns_calc AS (
    SELECT 
        symbol,
        index_name,
        AVG(
            (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / 
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100
        ) * 252 as annualized_return,
        STDDEV(
            (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / 
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100
        ) * SQRT(252) as annualized_volatility
    FROM index_daily_data 
    WHERE date >= (SELECT MAX(date) - INTERVAL '1 year' FROM index_daily_data)
    GROUP BY symbol, index_name
    HAVING COUNT(*) >= 200  -- 至少200个交易日
)
SELECT 
    symbol as 指数代码,
    index_name as 指数名称,
    ROUND(annualized_return, 2) as 年化收益率,
    ROUND(annualized_volatility, 2) as 年化波动率,
    ROUND((annualized_return - 3) / annualized_volatility, 4) as 夏普比率
FROM returns_calc
ORDER BY 夏普比率 DESC
LIMIT 15;

-- ========================================
-- 7. 数据质量检查查询
-- ========================================

-- 检查数据缺失情况
SELECT 
    symbol as 指数代码,
    COUNT(*) as 总记录数,
    SUM(CASE WHEN open IS NULL OR open = 0 THEN 1 ELSE 0 END) as 开盘价缺失,
    SUM(CASE WHEN high IS NULL OR high = 0 THEN 1 ELSE 0 END) as 最高价缺失,
    SUM(CASE WHEN low IS NULL OR low = 0 THEN 1 ELSE 0 END) as 最低价缺失,
    SUM(CASE WHEN close IS NULL OR close = 0 THEN 1 ELSE 0 END) as 收盘价缺失,
    SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as 成交量缺失
FROM index_daily_data
GROUP BY symbol
HAVING 开盘价缺失 > 0 OR 最高价缺失 > 0 OR 最低价缺失 > 0 OR 收盘价缺失 > 0 OR 成交量缺失 > 0
ORDER BY (开盘价缺失 + 最高价缺失 + 最低价缺失 + 收盘价缺失 + 成交量缺失) DESC;

-- 检查异常数据
SELECT 
    symbol as 指数代码,
    date as 日期,
    open as 开盘,
    high as 最高,
    low as 最低,
    close as 收盘,
    '价格异常' as 异常类型
FROM index_daily_data
WHERE high < low OR open < 0 OR high < 0 OR low < 0 OR close < 0
ORDER BY date DESC
LIMIT 20;