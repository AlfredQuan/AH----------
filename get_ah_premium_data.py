# -*- coding: utf-8 -*-
"""
Description:
A complete script for monitoring AH-share premiums for stocks included in the
Shanghai-Hong Kong Stock Connect program.

It consists of two main functions:
1. get_ah_shhk_connect_stocks: Fetches the eligible stock pool.
2. get_ah_premium_data: Fetches pricing data for the pool and calculates the premium.

Dependencies:
- WindPy, pandas, openpyxl
"""
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

# --- 全局Wind API连接 ---
try:
    from WindPy import w
    if not w.isconnected():
        w.start()
    WIND_AVAILABLE = True
    print("Wind接口连接成功。")
except Exception as e:
    print(f"错误: Wind接口连接失败: {e}")
    WIND_AVAILABLE = False

def get_ah_shhk_connect_stocks():
    """
    获取所有在A股上市且在沪港通内的H股股票池。
    
    该函数执行以下步骤:
    1. 初始化并连接到Wind API。
    2. 自动获取最新的交易日，以确保查询有效。
    3. 获取所有AH股的H股代码列表。
    4. 获取所有沪港通南向交易的H股代码列表。
    5. 计算两个列表的交集，得到最终的目标股票池（H股代码）。
    6. 获取这些H股对应的A股代码。
    7. 将H股代码、A股代码和股票名称整合到一个DataFrame中。
    8. 返回最终的DataFrame。

    Returns:
        pandas.DataFrame: 包含 'H_stock_code', 'A_stock_code', 'stock_name' 列的DataFrame。
                          如果连接失败或获取数据失败，则返回一个空的DataFrame。
    """
    try:
        from WindPy import w
        if not w.isconnected():
            w.start()
        print("Wind接口连接成功。")
    except ImportError:
        print("错误: WindPy模块未安装。请确保已安装Wind金融终端并配置好Python接口。")
        return pd.DataFrame()
    except Exception as e:
        print(f"错误: 连接到Wind接口时发生未知错误: {e}")
        return pd.DataFrame()

    # --- 步骤 1: 获取最新交易日 ---
    # 对于获取板块成分，使用最新交易日是必要的，以保证获取到最新的列表
    today_str = datetime.today().strftime('%Y-%m-%d')
    trade_day_data = w.tdaysoffset(0, today_str, "") # 获取距离today_str最近的交易日
    if trade_day_data.ErrorCode != 0:
        print(f"获取最新交易日失败，错误代码: {trade_day_data.ErrorCode}")
        return pd.DataFrame()
    
    latest_trade_day = trade_day_data.Data[0][0].strftime('%Y-%m-%d')
    print(f"当前日期: {today_str}，将使用最新交易日: {latest_trade_day} 进行查询。")


    # --- 步骤 2: 获取所有AH股的H股列表 ---
    print("\n步骤 2: 正在获取所有AH股的H股列表...")
    ah_stocks_data = w.wset("sectorconstituent", f"date={latest_trade_day};sectorid=a002010600000000")
    if ah_stocks_data.ErrorCode != 0:
        print(f"获取AH股列表失败，错误代码: {ah_stocks_data.ErrorCode}")
        return pd.DataFrame()
    
    ah_df = pd.DataFrame(ah_stocks_data.Data, index=ah_stocks_data.Fields).T
    if 'wind_code' not in ah_df.columns:
        print("错误：获取的AH股数据中不包含 'wind_code' 字段。")
        return pd.DataFrame()
        
    ah_stock_codes = set(ah_df['wind_code'])
    print(f"成功获取 {len(ah_stock_codes)} 只AH股。")

    # --- 步骤 3: 获取所有沪港通南向交易的H股列表 ---
    print("\n步骤 3: 正在获取所有沪港通南向交易的H股列表...")
    shhk_connect_data = w.wset("sectorconstituent", f"date={latest_trade_day};sectorid=1000014939000000")
    if shhk_connect_data.ErrorCode != 0:
        print(f"获取沪港通列表失败，错误代码: {shhk_connect_data.ErrorCode}")
        return pd.DataFrame()

    shhk_df = pd.DataFrame(shhk_connect_data.Data, index=shhk_connect_data.Fields).T
    if 'wind_code' not in shhk_df.columns:
        print("错误：获取的沪港通数据中不包含 'wind_code' 字段。")
        return pd.DataFrame()

    shhk_connect_codes = set(shhk_df['wind_code'])
    print(f"成功获取 {len(shhk_connect_codes)} 只沪港通标的股。")

    # --- 步骤 4: 找到两个列表的交集 ---
    print("\n步骤 4: 正在筛选同时在两个列表中的股票...")
    intersection_codes = sorted(list(ah_stock_codes.intersection(shhk_connect_codes)))
    if not intersection_codes:
        print("未找到同时满足条件的股票。")
        return pd.DataFrame()
    print(f"筛选完成，共找到 {len(intersection_codes)} 只符合条件的H股。")

    # --- 步骤 5: 获取交集股票对应的A股代码和股票名称 ---
    print("\n步骤 5: 正在获取这些H股对应的A股代码和股票名称...")
    fields_to_get = "ah_stockcode,sec_name"
    # 关键修正：对于获取ah_stockcode和sec_name这类基本资料，不应传入tradeDate参数。
    stock_info_data = w.wss(intersection_codes,"asharewindcode")
    if stock_info_data.ErrorCode != 0:
        print(f"获取A股代码和名称失败，错误代码: {stock_info_data.ErrorCode}")
        return pd.DataFrame()

    # --- 步骤 6: 整理数据到DataFrame ---
    print("\n步骤 6: 正在整理最终数据...")
    temp_df = pd.DataFrame({
        'H_stock_code': intersection_codes,
        'A_stock_code': stock_info_data.Data[0],
    })
    # 从最初的ah_df中提取H股代码和名称的映射关系
    name_map_df = ah_df[['wind_code', 'sec_name']].rename(columns={'wind_code': 'H_stock_code', 'sec_name': 'stock_name'})
    
    # 将A股代码与名称映射进行合并
    final_df = pd.merge(temp_df, name_map_df, on='H_stock_code', how='left')
    
    # 调整列的顺序，确保最终输出格式正确
    final_df = final_df[['H_stock_code', 'A_stock_code', 'stock_name']]
    
    final_df.dropna(subset=['A_stock_code'], inplace=True)
    print("数据整理完成。")
    
    return final_df

def get_ah_premium_data(stock_pool_df, start_date, end_date=None):
    """
    获取指定股票池在给定日期范围内的价格数据，并计算HA股溢价率。

    溢价率公式: (H股收盘价 * 沪港通汇率 / A股收盘价) - 1

    Args:
        stock_pool_df (pd.DataFrame): 包含 'A_stock_code', 'H_stock_code', 'stock_name' 的股票池。
        start_date (str): 开始日期，格式 "YYYY-MM-DD"。
        end_date (str, optional): 结束日期，格式 "YYYY-MM-DD"。如果为None，则只查询start_date单日。

    Returns:
        pd.DataFrame: 包含日期、代码、名称、价格、汇率和溢价率的详细数据。
                      如果失败则返回一个空的DataFrame。
    """
    if not WIND_AVAILABLE:
        print("Wind接口不可用，无法获取数据。")
        return pd.DataFrame()

    if stock_pool_df is None or stock_pool_df.empty:
        print("错误: 股票池为空，无法获取数据。")
        return pd.DataFrame()

    if end_date is None:
        end_date = start_date

    print(f"\n正在获取 {start_date} 到 {end_date} 的价格和汇率数据...")

    # --- 步骤 1: 批量获取所有A股和H股的收盘价 ---
    all_a_codes = stock_pool_df['A_stock_code'].tolist()
    all_h_codes = stock_pool_df['H_stock_code'].tolist()
    all_codes = all_a_codes + all_h_codes
    
    price_data = w.wsd(all_codes, "close", start_date, end_date, "")
    
    if price_data.ErrorCode != 0 or not price_data.Data or not price_data.Data[0]:
        print("获取股票价格数据失败，或返回的数据为空。")
        return pd.DataFrame()
    
    price_df = pd.DataFrame(price_data.Data, index=price_data.Codes, columns=price_data.Times).T
    price_df.index.name = 'date' 
    print(f"成功获取价格数据，转换后的price_df形状: {price_df.shape}")


    # --- 步骤 2: 获取沪港通汇率 ---
    # 关键修正：使用您提供的更准确的汇率代码
    exchange_rate_code = "HKDCNYFIX.HKS"
    exchange_rate_data = w.wsd(exchange_rate_code, "close", start_date, end_date, "")
    
    if exchange_rate_data.ErrorCode != 0 or not exchange_rate_data.Data or not exchange_rate_data.Data[0]:
        print(f"获取汇率数据({exchange_rate_code})失败，或返回的数据为空。")
        return pd.DataFrame()
        
    exchange_rate_df = pd.DataFrame(exchange_rate_data.Data, index=exchange_rate_data.Codes, columns=exchange_rate_data.Times).T
    exchange_rate_df.rename(columns={exchange_rate_code: 'exchange_rate'}, inplace=True)
    exchange_rate_df.index.name = 'date'
    
    # 由于这个汇率是每日更新的，我们不再需要复杂的向前填充逻辑
    print(f"成功获取汇率数据，转换后的exchange_rate_df形状: {exchange_rate_df.shape}")


    print("\n价格和汇率数据获取成功，正在进行整合与计算...")

    # --- 步骤 3: 数据整合 ---
    price_long_df = price_df.reset_index().melt(id_vars='date', var_name='code', value_name='price')
    a_price_df = price_long_df[price_long_df['code'].isin(all_a_codes)].rename(columns={'code': 'A_stock_code', 'price': 'A_price'})
    h_price_df = price_long_df[price_long_df['code'].isin(all_h_codes)].rename(columns={'code': 'H_stock_code', 'price': 'H_price'})
    merged_a = pd.merge(a_price_df, stock_pool_df, on='A_stock_code', how='inner')
    merged_prices = pd.merge(merged_a, h_price_df, on=['date', 'H_stock_code'], how='left')
    final_data = pd.merge(merged_prices, exchange_rate_df.reset_index(), on='date', how='left')
    
    # --- 步骤 4: 计算HA股溢价率 ---
    final_data['HA_premium_rate'] = np.where(
        final_data['A_price'].isnull() | (final_data['A_price'] == 0), 
        np.nan,
        (final_data['H_price']  / final_data['exchange_rate'] / final_data['A_price']) - 1
    )

    # 清理和排序
    final_data.dropna(subset=['A_price', 'H_price', 'exchange_rate'], inplace=True)
    
    if final_data.empty:
        print("警告: 清理数据后，没有可用的数据行。")
        return pd.DataFrame()

    final_data['date'] = pd.to_datetime(final_data['date'])
    final_data['date'] = final_data['date'].dt.strftime('%Y-%m-%d')
    final_data = final_data[[
        'date', 'stock_name', 'A_stock_code', 'H_stock_code', 
        'A_price', 'H_price', 'exchange_rate', 'HA_premium_rate'
    ]].sort_values(by=['date', 'stock_name']).reset_index(drop=True)

    print("数据处理和溢价率计算完成。")
    return final_data


def main():
    """
    主函数，演示如何执行完整流程。
    """
    if not WIND_AVAILABLE:
        print("\n无法执行，请检查Wind接口连接。")
        return

    # 1. 获取股票池
    print("--- 步骤 1: 获取股票池 ---")
    stock_pool = get_ah_shhk_connect_stocks()

    if stock_pool.empty:
        print("未能获取股票池，程序终止。")
        return
    
    # 2. 定义查询日期并获取溢价率数据
    print("\n--- 步骤 2: 获取溢价率数据 ---")
    # 示例：获取过去一周的数据
    end_date_str = datetime.today().strftime('%Y-%m-%d')
    start_date_str = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    premium_report = get_ah_premium_data(stock_pool, start_date_str, end_date_str)

    if not premium_report.empty:
        print("\n--- HA股溢价率报告 (部分数据预览) ---")
        print(premium_report.head(10))
        print("...")
        print(premium_report.tail(10))

        # 3. 保存到Excel
        try:
            filename = 'HA股溢价率报告.xlsx'
            premium_report.to_excel(filename, index=False, engine='openpyxl')
            print(f"\n报告已成功保存到文件: {filename}")
        except Exception as e:
            print(f"\n保存到Excel文件时出错: {e}")
    else:
        print("\n未能生成溢价率报告。")


if __name__ == "__main__":
    main()

