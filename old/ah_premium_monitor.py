import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
#warnings.filterwarnings('ignore')

# 假设Wind接口已经安装并可以使用
try:
    from WindPy import w
    w.start()
    wind_available = True
    print("Wind接口连接成功")
except ImportError:
    print("WindPy模块未安装，请先安装Wind金融终端并修复Python接口")
    wind_available = False
    exit()

class AHPremiumMonitor:
    def __init__(self, start_date="2023-01-01", end_date="2025-08-15"):
        """
        初始化AH股溢价率监控器
        
        Parameters:
        start_date (str): 开始日期，格式为"YYYY-MM-DD"
        end_date (str): 结束日期，格式为"YYYY-MM-DD"
        """
        self.start_date = start_date
        self.end_date = end_date
        self.stock_pool = None
        self.premium_data = None
        self.summary_data = None
        
    def get_AH_stocks(self):
        """
        获取同时在A股上市的港股股票池
        """
        if not wind_available:
            print("Wind接口不可用，程序无法继续执行")
            return

        
        # 使用test.py中的方法获取数据
        print("获取在A股上市的港股成分股...")
        today = datetime.today().strftime('%Y-%m-%d')
        
        try:
            # 获取在A股上市的港股（直接使用专门的板块ID）
            print("正在获取在A股上市的港股...")
            AH = w.wset("sectorconstituent", f"date={today};sectorid=a002010600000000")
            
            if AH.ErrorCode == 0:
                print("成功获取在A股上市的香港成分股数据")
                self._process_ah_stocks(AH)
                return AH
            
        except Exception as e:
            print(f"使用test.py方法时出错: {e}")
            import traceback
            traceback.print_exc()
            
        
        
    def _process_ah_stocks(self, AH_data):
        """
        处理直接从wset获取的AH股数据
        """
        try:
            print("正在处理直接获取的AH股数据...")
            
            # 检查数据结构
            if not hasattr(AH_data, 'Data') or len(AH_data.Data) < 2:
                print("AH股数据格式不正确")
                return
                
            print(f"AH股数据字段: {AH_data.Fields}")
            print(f"AH股数据条数: {len(AH_data.Data[2]) if AH_data.Data else 0}")
            
            # 构建DataFrame
            if len(AH_data.Data) >= 3:
                AH_df = pd.DataFrame({
                    'date': AH_data.Data[0],
                    'wind_code': AH_data.Data[1],
                    'sec_name': AH_data.Data[2]
                })
            
            else:
                print("AH股数据不足，无法处理")
                return
            
            print(f"获取到 {len(AH_df)} 只在A股上市的港股")
            ah_stocks = AH_df
            return ah_stocks
            
        except Exception as e:
            print(f"处理直接获取的AH股数据时出错: {e}")
            import traceback
            traceback.print_exc()
    
  
    
    def _filter_sh_hk_connect(self, ah_stocks):
        """
        筛选在沪港通中的股票
        """
        try:
            print("正在筛选沪港通中的股票...")
            today = datetime.today().strftime('%Y-%m-%d')
            
            # 获取沪港通成分股
            print("正在获取沪港通成分股...")
            HGT = w.wset("sectorconstituent", f"date={today};sectorid=1000014939000000")
            
            # 处理沪港通数据
            if hasattr(HGT, 'Data') and len(HGT.Data) >= 2:
                HGT_df = pd.DataFrame({
                    'date': HGT.Data[0],
                    'wind_code': HGT.Data[1],
                    'sec_name': HGT.Data[2] if len(HGT.Data) > 2 else None
                })
                print(f"获取到 {len(HGT_df)} 只沪港通股票")
                
            HGT_stocks = HGT_df

            
            # 筛选出同时在沪港通中的AH股
            filtered_stocks = list(filter(lambda x: x['wind_code'] in HGT_stocks['wind_code'].tolist(), ah_stocks))
            
            if len(filtered_stocks) > 0:
                self.stock_pool = pd.DataFrame(filtered_stocks)
                print(f"成功筛选出在沪港通中的AH股，共 {len(self.stock_pool)} 只股票")
                print(self.stock_pool.head(10))
            else:
                print("未找到在沪港通中的AH股")

        except Exception as e:
            print(f"筛选沪港通股票时出错: {e}")
            import traceback
            traceback.print_exc()

    
    def fetch_stock_data(self):
        """
        获取股票池中股票在指定时间区间内的每日收盘价和汇率
        """
        if self.stock_pool is None:
            self.get_stock_pool()
            
        if not wind_available:
            print("Wind接口不可用，程序无法继续执行")
            return
            
        if self.stock_pool is None or len(self.stock_pool) == 0:
            print("股票池为空，无法获取价格数据")
            return
            
        try:
            self._fetch_real_data()
        except Exception as e:
            print(f"获取实时数据时出错: {e}")
            import traceback
            traceback.print_exc()
    
    def _fetch_real_data(self):
        """
        通过Wind接口获取真实数据
        """
        print("正在获取交易日数据...")
        # 获取交易日期
        trade_days = w.tdays(self.start_date, self.end_date, "")
        if trade_days.ErrorCode != 0:
            print(f"获取交易日数据失败，错误代码: {trade_days.ErrorCode}")
            return
            
        dates = [d.strftime("%Y-%m-%d") for d in trade_days.Data[0]]
        print(f"获取到 {len(dates)} 个交易日的数据")
        
        # 初始化数据存储
        data_list = []
        
        total_stocks = len(self.stock_pool)
        print(f"开始获取 {total_stocks} 只股票的价格数据...")
        
        # 遍历股票池中的每只股票
        for idx, (_, row) in enumerate(self.stock_pool.iterrows(), 1):
            a_code = row['A_stock_code']
            h_code = row['H_stock_code']
            name = row['stock_name']
            
            print(f"正在获取第 {idx}/{total_stocks} 只股票 {name} ({a_code}/{h_code}) 的数据...")
            
            # 获取A股收盘价
            a_price_data = w.wsd(a_code, "close", self.start_date, self.end_date, "")
            # 获取H股收盘价
            h_price_data = w.wsd(h_code, "close", self.start_date, self.end_date, "")
            # 获取汇率（港币兑人民币汇率）
            exchange_rate_data = w.wsd("HKD.CNY", "close", self.start_date, self.end_date, "")
            
            if a_price_data.ErrorCode == 0 and h_price_data.ErrorCode == 0 and exchange_rate_data.ErrorCode == 0:
                successful_entries = 0
                for i, date in enumerate(dates):
                    if i < len(a_price_data.Data[0]) and i < len(h_price_data.Data[0]) and i < len(exchange_rate_data.Data[0]):
                        a_price = a_price_data.Data[0][i]
                        h_price = h_price_data.Data[0][i]
                        exchange_rate = exchange_rate_data.Data[0][i]
                        
                        # 只有当数据有效时才添加
                        if a_price is not None and h_price is not None and exchange_rate is not None and a_price > 0:
                            data_list.append({
                                'date': date,
                                'stock_name': name,
                                'A_stock_code': a_code,
                                'H_stock_code': h_code,
                                'A_price': a_price,
                                'H_price': h_price,
                                'exchange_rate': exchange_rate
                            })
                            successful_entries += 1
                print(f"  获取到 {name} 的 {successful_entries} 条有效数据")
            else:
                print(f"  获取 {name} 数据时出错: A股错误码{a_price_data.ErrorCode}, H股错误码{h_price_data.ErrorCode}, 汇率错误码{exchange_rate_data.ErrorCode}")
        
        self.premium_data = pd.DataFrame(data_list)
        print(f"总共获取到 {len(self.premium_data)} 条价格数据")
        
    def calculate_premium(self):
        """
        计算每个交易日的AH溢价率
        公式: H股收盘价 * 汇率 / A股收盘价 - 1
        """
        if self.premium_data is None or len(self.premium_data) == 0:
            print("没有可用的价格数据，无法计算溢价率")
            return
            
        self.premium_data['premium_rate'] = (
            self.premium_data['H_price'] * self.premium_data['exchange_rate'] / self.premium_data['A_price'] - 1
        )
        print("AH溢价率计算完成")
        
    def calculate_summary(self):
        """
        计算各公司在时间区间内的AH溢价率统计值：
        最小值、5%分位数、95%分位数、最大值
        """
        if self.premium_data is None or len(self.premium_data) == 0:
            print("没有可用的溢价率数据，无法计算统计摘要")
            return
            
        summary_list = []
        for name in self.premium_data['stock_name'].unique():
            stock_data = self.premium_data[self.premium_data['stock_name'] == name]
            premium_rates = stock_data['premium_rate'].dropna()
            
            if len(premium_rates) > 0:
                summary = {
                    'stock_name': name,
                    'min_premium': premium_rates.min(),
                    'percentile_5': np.percentile(premium_rates, 5),
                    'percentile_95': np.percentile(premium_rates, 95),
                    'max_premium': premium_rates.max()
                }
                summary_list.append(summary)
        
        self.summary_data = pd.DataFrame(summary_list)
        print("统计摘要计算完成")
        
    def save_to_excel(self, filename='ah_premium_report.xlsx'):
        """
        将结果保存到Excel文件中
        """
        if self.summary_data is None or len(self.summary_data) == 0:
            print("没有汇总数据可保存")
            return
            
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # 保存详细数据
            if self.premium_data is not None and len(self.premium_data) > 0:
                self.premium_data.to_excel(writer, sheet_name='详细数据', index=False)
            # 保存汇总数据
            self.summary_data.to_excel(writer, sheet_name='汇总统计', index=False)
            
        print(f"数据已保存到 {filename}")
        
    def daily_update(self):
        """
        动态更新：每日抓取最新数据
        """
        if not wind_available:
            print("Wind接口不可用，无法进行实时数据更新")
            return
            
        # 获取昨天的日期
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        try:
            # 获取最新一日的数据
            self._fetch_latest_data(yesterday)
            print(f"已更新 {yesterday} 的数据")
        except Exception as e:
            print(f"更新数据时出错: {e}")
            
    def _fetch_latest_data(self, date):
        """
        获取指定日期的最新数据
        """
        # 这里需要实现获取单日数据的逻辑
        # 实际使用时需要根据Wind接口的具体语法进行调整
        pass
    
    def run(self):
        """
        运行完整流程
        """
        print("开始获取AH股股票池...")
        self.get_AH_stocks()
        self.stock_pool = self._filter_sh_hk_connect(self._process_ah_stocks(self.get_AH_stocks()))

        
        if self.stock_pool is None or len(self.stock_pool) == 0:
            print("未能获取到股票池，程序终止")
            #self.stock_pool
            return
        
        print("开始获取股票价格和汇率数据...")
        self.fetch_stock_data()
        
        
        if self.premium_data is None or len(self.premium_data) == 0:
            print("未能获取到价格数据，程序终止")
            return
            
        print("计算AH溢价率...")
        self.calculate_premium()
        
        print("计算统计摘要...")
        self.calculate_summary()
        
        print("生成报告...")
        print("\n=== AH股溢价率统计摘要 ===")
        if self.summary_data is not None and len(self.summary_data) > 0:
            print(self.summary_data.to_string(index=False))
        else:
            print("暂无统计结果")
        
        # 保存到Excel
        self.save_to_excel()
        
        print("\n任务完成!")

def main():
    # 创建AH股溢价率监控器实例
    monitor = AHPremiumMonitor(start_date="2023-01-01", end_date="2025-08-15")
    
    # 运行完整流程
    monitor.run()
    
    # 如果需要每日更新，可以调用以下方法
    # monitor.daily_update()

if __name__ == "__main__":
    main()