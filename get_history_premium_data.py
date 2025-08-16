# -*- coding: utf-8 -*-
"""
Description:
This is the main analysis script for the AH-share premium project.
It imports functions from 'get_ah_premium_data.py' to perform the following steps:
1. Fetch the eligible stock pool.
2. Fetch the detailed premium data for a specified date range.
3. Calculate summary statistics for the premium rates of each stock.
4. Save both the detailed data and the summary statistics into one Excel file.

To run this script:
- Make sure this file is in the same directory as 'get_ah_premium_data.py'.
- Set the START_DATE and END_DATE variables below.
"""
import pandas as pd
from datetime import datetime

# 导入我们之前创建的模块中的函数
# 请确保 get_ah_premium_data.py 文件与此脚本在同一目录下
try:
    from get_ah_premium_data import get_ah_shhk_connect_stocks, get_ah_premium_data
except ImportError:
    print("错误: 无法导入 'get_ah_premium_data.py'。")
    print("请确保该文件与本脚本位于同一目录下。")
    exit()

def calculate_premium_statistics(premium_df):
    """
    计算每只股票在时间区间内的HA股溢价率的各项统计值。

    Args:
        premium_df (pd.DataFrame): 包含详细溢价率数据的DataFrame。

    Returns:
        pd.DataFrame: 包含各股票溢价率统计摘要的DataFrame。
    """
    if premium_df is None or premium_df.empty:
        print("没有可供分析的数据，无法计算统计值。")
        return pd.DataFrame()

    print("\n正在计算溢价率的统计摘要...")

    # 定义需要计算的统计函数
    # 使用 a.quantile(x) 来计算分位数
    agg_funcs = {
        'HA_premium_rate': [
            'mean',                             # 平均值
            'median',                           # 中位数
            'min',                              # 最小值
            'max',                              # 最大值
            lambda x: x.quantile(0.05),         # 5%分位数
            lambda x: x.quantile(0.95),         # 95%分位数
            'std'                               # 标准差 (作为参考)
        ]
    }

    # 按股票分组并应用统计函数
    # stock_name, A_stock_code, H_stock_code 是静态信息，可以作为分组键
    stats_df = premium_df.groupby(['stock_name', 'A_stock_code', 'H_stock_code']).agg(agg_funcs)

    # 清理列名，使其更易读
    stats_df.columns = [
        'premium_mean', 
        'premium_median', 
        'premium_min', 
        'premium_max', 
        'premium_p5', 
        'premium_p95', 
        'premium_std'
    ]
    
    # 将多级索引重置为普通列
    stats_df = stats_df.reset_index()
    
    print("统计摘要计算完成。")
    return stats_df

def main():
    """
    主执行函数
    """
    # --- 参数设置 ---
    # 请在这里设置您想分析的开始和结束日期
    START_DATE = "2024-01-01"
    END_DATE = datetime.today().strftime('%Y-%m-%d')
    OUTPUT_FILENAME = f'AH股溢价率分析报告_{START_DATE}_至_{END_DATE}.xlsx'

    print("--- 开始执行AH股溢价率分析脚本 ---")

    # 1. 获取股票池
    print("\n--- 步骤 1: 获取股票池 ---")
    stock_pool = get_ah_shhk_connect_stocks()
    if stock_pool.empty:
        print("未能获取股票池，程序终止。")
        return

    # 2. 获取指定时间区间的详细溢价率数据
    print("\n--- 步骤 2: 获取详细溢价率数据 ---")
    premium_details = get_ah_premium_data(stock_pool, START_DATE, END_DATE)
    if premium_details.empty:
        print("未能获取详细溢价率数据，程序终止。")
        return

    # 3. 计算统计摘要
    premium_stats = calculate_premium_statistics(premium_details)
    if premium_stats.empty:
        print("未能计算统计摘要，程序终止。")
        return

    # --- 结果展示与保存 ---
    print("\n--- 溢价率统计摘要 (部分预览) ---")
    print(premium_stats.head())

    print(f"\n正在将结果保存到Excel文件: {OUTPUT_FILENAME}")
    try:
        # 使用ExcelWriter可以在同一个文件中写入多个sheet
        with pd.ExcelWriter(OUTPUT_FILENAME, engine='openpyxl') as writer:
            premium_stats.to_excel(writer, sheet_name='统计摘要', index=False)
            premium_details.to_excel(writer, sheet_name='详细数据', index=False)
        print("报告已成功保存！")
    except Exception as e:
        print(f"保存Excel文件时出错: {e}")

    print("\n--- 脚本执行完毕 ---")


if __name__ == "__main__":
    main()
