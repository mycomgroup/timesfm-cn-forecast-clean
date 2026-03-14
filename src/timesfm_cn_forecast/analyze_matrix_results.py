#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""汇总实验矩阵结果。"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

def analyze_matrix(matrix_dir: str):
    root_path = Path(matrix_dir)
    if not root_path.exists():
        print(f"目录不存在: {matrix_dir}")
        return

    all_summaries = []

    # 遍历目录下所有以 results_ 开头的 CSV 文件
    results_files = list(root_path.glob("**/results_*.csv"))
    if not results_files:
        # 兼容旧版本目录结构
        results_files = list(root_path.glob("**/results.csv"))

    for results_path in results_files:
        df = pd.read_csv(results_path)
        # 过滤状态成功的个股
        valid_df = df[df["status"] == "ok"]
        if valid_df.empty:
            continue

        # 从 DataFrame 中提取参数
        fs = df["feature_set"].iloc[0] if "feature_set" in df.columns else "unknown"
        td = df["train_days"].iloc[0] if "train_days" in df.columns else "unknown"
        h = df["horizon"].iloc[0] if "horizon" in df.columns else "unknown"
        cl = df["context_len"].iloc[0] if "context_len" in df.columns else "unknown"

        summary = {
            "File": results_path.name,
            "FeatureSet": fs,
            "TrainDays": td,
            "Horizon": h,
            "ContextLen": cl,
            "AvgHitRate": valid_df["hitrate"].mean(),
            "AvgRMSE": valid_df["rmse"].mean(),
            "Consistency": valid_df["hitrate"].std(),
            "ValidStocks": len(valid_df),
            "SuccessRate": len(valid_df) / len(df) * 100
        }
        all_summaries.append(summary)

    if not all_summaries:
        print("未找到有效的实验结果。")
        return

    summary_df = pd.DataFrame(all_summaries)
    
    # 排序：优选胜率高且一致性好（标准差小）的项目
    summary_df = summary_df.sort_values(by=["AvgHitRate", "Consistency"], ascending=[False, True])

    print("\n" + "="*80)
    print("实验矩阵汇总报告")
    print("="*80)
    print(summary_df.to_string(index=False))
    print("="*80)

    output_file = root_path / "matrix_summary.csv"
    summary_df.to_csv(output_file, index=False)
    print(f"\n结果已保存至: {output_file}")

    # 推荐参数
    best = summary_df.iloc[0]
    print(f"\n推荐最优组合:")
    print(f"  特征集: {best['FeatureSet']}")
    print(f"  训练天数: {best['TrainDays']}")
    print(f"  预测跨度: {best['Horizon']}")
    print(f"  平均胜率: {best['AvgHitRate']:.2f}%")
    print(f"  一致性(StdDev): {best['Consistency']:.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, required=True, help="矩阵实验根目录")
    args = parser.parse_args()
    analyze_matrix(args.dir)
