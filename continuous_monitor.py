# -*- coding: utf-8 -*-
"""
Description:
本脚本用于持续监控AH股溢价率。首次运行时，它会创建一个包含历史数据的Excel报告。
在后续运行中，它会自动检测并使用最新的每日数据来更新报告。

Excel报告包含四张表:
1. 溢价率
2. A股收盘价
3. H股收盘价
4. 汇率

每张表的格式均为：日期为列（横轴），股票名称为行（纵轴）。

使用方法:
- 将本文件与 'get_ah_premium_data.py' 放置在同一目录下。
- 定期（如每日）运行此脚本以自动更新报告。
"""
import pandas as pd
from datetime import datetime, timedelta
import os

# 导入我们之前创建的模块中的函数
try:
    # 直接使用原始的数据获取函数，并导入w和WIND_AVAILABLE用于日期检查
    from get_ah_premium_data import get_ah_shhk_connect_stocks, get_ah_premium_data, w, WIND_AVAILABLE
except ImportError:
    print("错误: 无法导入 'get_ah_premium_data.py'。")
    print("请确保该文件与本脚本位于同一目录下。")
    exit()

def create_pivot_table(df, value_col):
    """将长表数据转换为宽表（透视表）"""
    # 在透视之前，处理可能因同一天同一股票出现多次（理论上不应发生）导致的问题
    # 通过去重保留第一个出现的记录
    df_dedup = df.drop_duplicates(subset=['stock_name', 'date'])
    
    pivot = df_dedup.pivot_table(index='stock_name', columns='date', values=value_col)
    pivot.columns = pd.to_datetime(pivot.columns).strftime('%Y-%m-%d')
    return pivot

def main():
    """主执行函数"""
    OUTPUT_FILENAME = 'AH股每日监控报告.xlsx'
    SHEET_NAMES = ['溢价率', 'A股收盘价', 'H股收盘价', '汇率']
    
    print("--- 开始执行AH股每日监控脚本 ---")

    # 1. 获取股票池
    print("\n--- 步骤 1: 获取股票池 ---")
    stock_pool = get_ah_shhk_connect_stocks()
    if stock_pool.empty:
        print("未能获取股票池，程序终止。")
        return

    # 2. 检查现有文件，确定需要获取的数据日期范围
    start_date_str = "2024-01-01" # 首次运行时的默认开始日期
    existing_data = {}
    if os.path.exists(OUTPUT_FILENAME):
        try:
            print(f"检测到现有报告 '{OUTPUT_FILENAME}'，将进行增量更新。")
            # 读取现有数据以确定更新起点
            # 我们只需要读取一张表来确定最后一个日期即可
            existing_premium = pd.read_excel(OUTPUT_FILENAME, sheet_name=SHEET_NAMES[0], index_col=0)
            last_date_in_file = pd.to_datetime(existing_premium.columns[-1])
            start_date_str = (last_date_in_file + timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 保存所有旧数据以备合并
            for sheet in SHEET_NAMES:
                existing_data[sheet] = pd.read_excel(OUTPUT_FILENAME, sheet_name=sheet, index_col=0)

        except Exception as e:
            print(f"读取现有文件失败: {e}。将重新生成报告。")
            existing_data = {} # 清空数据，确保重新生成
            
    end_date_str = datetime.today().strftime('%Y-%m-%d')
    
    # --- 关键修正：检查是否有新的交易日数据可供更新 ---
    if WIND_AVAILABLE:
        latest_trade_day_data = w.tdaysoffset(0, end_date_str, "")
        if latest_trade_day_data.ErrorCode == 0 and latest_trade_day_data.Data:
            latest_available_date_str = latest_trade_day_data.Data[0][0].strftime('%Y-%m-%d')
            # 如果我们想开始更新的日期 晚于 最后一个有数据的日期，说明没有新数据
            if start_date_str > latest_available_date_str:
                print(f"\n数据已是最新，最新数据日期为 {latest_available_date_str}。无需更新。")
                print("--- 脚本执行完毕 ---")
                return
    
    if start_date_str > end_date_str:
        print("\n数据已是最新，无需更新。")
        print("--- 脚本执行完毕 ---")
        return

    # 3. 获取新数据 (使用原始的get_ah_premium_data函数)
    long_df = get_ah_premium_data(stock_pool, start_date_str, end_date_str)
    if long_df.empty:
        print("未能获取到新的数据，可能是当日数据尚未更新。程序终止。")
        return

    # 4. 创建新的透视表
    value_columns = ['HA_premium_rate', 'A_price', 'H_price', 'exchange_rate']
    new_pivots = {}
    for sheet, value_col in zip(SHEET_NAMES, value_columns):
        new_pivots[sheet] = create_pivot_table(long_df, value_col)

    # 5. 合并新旧数据
    final_pivots = {}
    if existing_data:
        print("正在合并新旧数据...")
        for sheet in SHEET_NAMES:
            final_pivots[sheet] = pd.concat([existing_data[sheet], new_pivots[sheet]], axis=1)
    else:
        print("正在创建新报告...")
        final_pivots = new_pivots
        
    # 6. 保存到Excel
    print(f"\n正在将结果保存到Excel文件: {OUTPUT_FILENAME}")
    try:
        with pd.ExcelWriter(OUTPUT_FILENAME, engine='openpyxl') as writer:
            for sheet_name, df_to_save in final_pivots.items():
                df_to_save.to_excel(writer, sheet_name=sheet_name)
        print("报告已成功保存！")
    except Exception as e:
        print(f"保存Excel文件时出错: {e}")

    print("\n--- 脚本执行完毕 ---")

if __name__ == "__main__":
    main()
