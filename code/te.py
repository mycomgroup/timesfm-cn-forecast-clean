# -*- coding: utf-8 -*-
import logging
import json
import base64
# -*- coding: utf-8 -*-
import logging
import os
import json
import base64
import akshare as ak
import pandas as pd
# from modelscope.hub.api import HubApi
from datetime import datetime, timedelta
import time
import random


# 配置常量（注意替换为你自己的）
OWNER_NAME = 'yuping322'
DATASET_NAME = 'stock_zh_a_daily'
LOCAL_DIR = '/tmp/ak_data'  # 阿里云函数计算中临时目录建议放 /tmp
# CSV_PATH = os.path.join(LOCAL_DIR, 'stock_zh_a_spot_em.csv')
ACCESS_TOKEN = 'ms-d8e9a746-ae24-4b2c-b632-56a1cd39756d'  # 推荐存为环境变量 os.getenv("MODELSCOPE_TOKEN")


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

output_dir = "/home/data/daily_data"
ensure_dir(output_dir)

def upload_dataset(file_path,filename):
    api = HubApi()
    api.login(ACCESS_TOKEN)

    api.upload_file(
    path_or_fileobj=file_path,
    path_in_repo=filename,
    repo_id=f"{OWNER_NAME}/{DATASET_NAME}",
    repo_type = 'dataset',
    commit_message='upload dataset file to repo',
)


def print_with_time(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_all_symbols():
    stock_list_file = "all_a_stocks.csv"
    if os.path.exists(stock_list_file):
        print_with_time("读取本地股票列表文件 all_a_stocks.csv ...")
        stock_list_df = pd.read_csv(stock_list_file)
    else:
        print_with_time("下载A股列表 ...")
        stock_list_df = ak.stock_info_a_code_name()
        stock_list_df.to_csv(stock_list_file, index=False)
        print_with_time("A股列表已保存到 all_a_stocks.csv")
    # 自动加前缀
    def add_prefix(code):
        code = str(code).zfill(6)
        if code.startswith("6"):
            return "sh" + code
        elif code.startswith(("0", "3")):
            return "sz" + code
        elif code.startswith(("4", "8")):
            return "bj" + code
        else:
            return code
    stock_list_df["full_symbol"] = stock_list_df["code"].apply(add_prefix)
    return stock_list_df[["full_symbol", "name"]]

def get_last_date(df):
    if "日期" in df.columns:
        return df["日期"].max()
    elif "date" in df.columns:
        return df["date"].max()
    return None


def download_and_update(symbol, name, output_dir, adjust, default_start_date, today, sleep_sec=1):
    file_path = os.path.join(output_dir, f"{symbol}.csv")
    old_df = None
    if os.path.exists(file_path):
        print_with_time(f"检测到已存在文件 {file_path}，准备增量更新 ...")
        try:
            old_df = pd.read_csv(file_path)
            last_date = get_last_date(old_df)
            if last_date is None or pd.isna(last_date):
                start_date = default_start_date
            else:
                try:
                    last_date_dt = pd.to_datetime(str(last_date))
                except Exception:
                    last_date_dt = datetime.strptime(str(last_date), "%Y-%m-%d")
                next_date = (last_date_dt + timedelta(days=1)).strftime("%Y%m%d")
                start_date = next_date
        except Exception as e:
            print_with_time(f"Error reading {file_path}: {e}")
            start_date = default_start_date
    else:
        start_date = default_start_date

    if start_date > today:
        print_with_time(f"Skip {symbol}, already up to date.")
        return False

    try:
        print_with_time(f"下载 {symbol}（{name}）: {start_date} ~ {today} ...")
        df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=today, adjust=adjust)
        if "date" in df.columns:
            df.rename(columns={"date": "日期"}, inplace=True)
        if df.empty:
            print_with_time(f"No new data for {symbol}")
            return
        df["symbol"] = symbol
        df["name"] = name
        df["adjust"] = adjust

        if old_df is not None:
            combined = pd.concat([old_df, df], ignore_index=True)
            if "日期" in combined.columns:
                combined = combined.drop_duplicates(subset=["日期"])
            combined.to_csv(file_path, index=False)
            print_with_time(f"Appended new data to {file_path}")
        else:
            df.to_csv(file_path, index=False)
            print_with_time(f"Saved {file_path}")
        time.sleep(sleep_sec)
        return True
    except Exception as e:
        print_with_time(f"Error for {symbol}: {e}")
        return False

def handler(event, context):
    logger = logging.getLogger()
    logger.info("receive event: %s", event)

import json
import logging

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

import json
import logging

logger = logging.getLogger()

def handler(event, context):
    """
    阿里云函数计算入口，兼容 FC 事件包装和本地裸 dict
    """
    # -------- 1. 先把 event 变成 dict --------
    if isinstance(event, bytes):
        event = json.loads(event.decode('utf-8'))
    elif isinstance(event, str):
        event = json.loads(event)
    # 已经是 dict 保持不变

    # -------- 2. 如果是 FC 事件，再剥 "body" --------
    if "body" in event:
        # FC HTTP 触发器会把真正的 payload 放在 event["body"] 里
        body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
    else:
        # 本地调试或裸 dict
        body = event

    # -------- 3. 取 stock_list --------
    stock_list = body.get("stock_list")
    if not stock_list:
        msg = "event 中缺少 stock_list 字段"
        logger.error(msg)
        return {"error": msg}

    logger.info("待处理股票列表: %s", stock_list)

    # -------- 4. 你的业务逻辑写在这里 --------
    # ...

    # return {"success": True, "count": len(stock_list)}

    # 后续业务逻辑：遍历
    # results = []
    # for item in stock_list:

    # 2. 后续业务逻辑
    # for code in stock_list:
    #     do_something(code)

    # return {"stock_list": stock_list, "status": "ok"}

    # stock_list = get_all_symbols()
    # # stock_list = stock_list.sample(n=100, random_state=random.randint(0, 10000)).reset_index(drop=True)

    # # 随机决定是正序还是倒序
    # if random.choice([True, False]):
    #     stock_list = stock_list.sort_values(by=stock_list.columns[0], ascending=True).reset_index(drop=True)
    # else:
    #     stock_list = stock_list.sort_values(by=stock_list.columns[0], ascending=False).reset_index(drop=True)


    for row in stock_list:           # row 就是 dict
        try:
            symbol = row['code']
            if symbol.startswith(('600','601','603','605','688')):
                symbol_with_prefix = f"sh{symbol}"
            elif symbol.startswith(('000','001','002','003','30')):
                symbol_with_prefix = f"sz{symbol}"
            elif symbol.startswith(('82',)):  # 优先股
                symbol_with_prefix = f"bj{symbol}"
            elif symbol.startswith('920'):  # 新编码存量公司 & 新增上市公司
                symbol_with_prefix = f"bj{symbol}"
            elif symbol.startswith(('83','87','88')):  # 普通股票旧编码
                symbol_with_prefix = f"bj{symbol}"
            elif symbol.startswith('43'):  # 旧三板延续，逐步向920迁移
                symbol_with_prefix = f"bj{symbol}"
            elif symbol[0].isdigit():  # 数字
                symbol_with_prefix = f"bj{symbol}"
            else:
                symbol_with_prefix = f"{symbol}"  # 北交所、新三板精选层等
            symbol=symbol_with_prefix

            print_with_time(f"处理股票 {symbol} ...")
            name = row['name']
            adjust = "qfq"
            default_start_date = "19910403"
            today = datetime.today().strftime("%Y%m%d")

            # download_dataset(symbol)
            flag = download_and_update(symbol, name, output_dir, adjust, default_start_date, today)
            file_path = os.path.join(output_dir, f"{symbol}.csv")

            
        except Exception as e:
            print_with_time(f"Error for {symbol}: {e}")
            continue
    print_with_time(f"处理完成 ...")
    return {"stock_list": stock_list, "status": "ok"}


# ==== 本地调试 ====
if __name__ == "__main__":
    fake_event = {
        "stock_list": [
            {"code": "000001", "name": "平安银行"},
            {"code": "600519", "name": "贵州茅台"},
            {"code": "300750", "name": "宁德时代"}
        ]
    }
    fake_context = {}

    result = handler(fake_event, fake_context)
    print("Result:")
    print(result)
