# 1. 导入所需库并启动Wind API
import pandas as pd
from WindPy import w
from datetime import datetime

# 启动Wind API，并检查连接状态
if not w.isconnected():
    print("正在启动Wind API...")
    w.start()
    if not w.isconnected():
        print("Wind API 启动失败，请检查万得终端是否已登录。")
        exit() # 如果无法连接则退出脚本

# 设置查询日期为今天
today = datetime.today().strftime('%Y-%m-%d')
print(f"查询日期: {today}\n")

try:
    # 2. 获取所有A+H股列表
    # 'a001010h00000000' 是Wind中"A+H股"板块的ID
    print("步骤1: 正在获取所有A+H股列表...")
    ah_sector_data = w.wset("sectorconstituent", f"date={today};sectorid=a001010h00000000")

    if ah_sector_data.ErrorCode != 0:
        print(f"获取A+H股列表失败: {ah_sector_data.Data}")
    else:
        a_share_codes = ah_sector_data.Data[1]
        if not a_share_codes:
            print("未能获取到A+H股列表，请检查日期或板块ID。")
        else:
            print(f"成功获取 {len(a_share_codes)} 只A+H股的A股代码。\n")

            # 3. 获取A股对应的H股代码和公司简称
            print("步骤2: 正在查询对应的H股代码和公司简称...")
            # 【关键修正】使用正确的指标 'clause_ah_corresponding_code'
            ah_details_data = w.wss(a_share_codes, "sec_name,clause_ah_corresponding_code")
            
            if ah_details_data.ErrorCode != 0:
                print(f"获取H股对应代码失败: {ah_details_data.Data}")
            else:
                # 将结果整理成DataFrame，方便后续处理
                ah_stocks_df = pd.DataFrame(ah_details_data.Data, index=ah_details_data.Fields, columns=ah_details_data.Codes).T
                ah_stocks_df.index.name = 'A_Share_Code'
                ah_stocks_df.rename(columns={'SEC_NAME': 'Stock_Name', 'CLAUSE_AH_CORRESPONDING_CODE': 'H_Share_Code'}, inplace=True)
                ah_stocks_df.dropna(subset=['H_Share_Code'], inplace=True)
                print(f"成功匹配到 {len(ah_stocks_df)} 对A+H股信息。\n")

                # 4. 获取沪港通和港股通（沪）的标的列表
                print("步骤3: 正在获取沪港通和港股通(沪)标的列表...")
                sh_connect_data = w.wset("sectorconstituent", f"date={today};sectorid=1000014553000000")
                hk_connect_sh_data = w.wset("sectorconstituent", f"date={today};sectorid=1000014554000000")

                if sh_connect_data.ErrorCode != 0 or hk_connect_sh_data.ErrorCode != 0:
                    print("获取沪港通或港股通(沪)标的失败。")
                else:
                    northbound_set = set(sh_connect_data.Data[1])
                    southbound_sh_set = set(hk_connect_sh_data.Data[1])
                    print(f"获取到 {len(northbound_set)} 只沪股通标的，{len(southbound_sh_set)} 只港股通(沪)标的。\n")

                    # 5. 筛选出最终结果
                    print("步骤4: 正在筛选最终股票列表...")
                    final_results = []
                    for index, row in ah_stocks_df.iterrows():
                        a_code = index
                        h_code = row['H_Share_Code']
                        
                        is_in_northbound = a_code in northbound_set
                        is_in_southbound = h_code in southbound_sh_set
                        
                        if is_in_northbound or is_in_southbound:
                            final_results.append({
                                'A股代码': a_code,
                                'H股代码': h_code,
                                '股票名称': row['Stock_Name'],
                                '是否沪股通标的': '是' if is_in_northbound else '否',
                                '是否港股通(沪)标的': '是' if is_in_southbound else '否'
                            })
                    
                    final_df = pd.DataFrame(final_results)

                    # 6. 显示结果
                    print("\n查询完成！同时在A+H上市且包含在沪港通中的股票如下：")
                    print(f"总计: {len(final_df)} 只")
                    pd.set_option('display.max_rows', 150)
                    print(final_df)

finally:
    # 7. 停止Wind API，释放资源
    w.stop()
    print("\nWind API 已停止。")