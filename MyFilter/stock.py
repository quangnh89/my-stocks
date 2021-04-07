import pandas as pd
import pymysql
import talib


class Stock:
    def __init__(self, code, resolution='D'):
        self.code = code
        self.resolution = resolution
        self.conn = pymysql.connect(host='localhost', user='admin', password='123456', database='mystocks')

        # Load prices
        if resolution == 'D':
            tbl = 'tbl_price_board_day'
        elif resolution == 'H':
            tbl = 'tbl_price_board_hour'
        elif resolution == 'M':
            tbl = 'tbl_price_board_minute'
        else:
            tbl = 'tbl_price_board_day'

        # Load finance info
        sql_finance_info = """select * from tbl_finance_info as ti where ti.code='"""+code+"""' order by year_period desc, quarter_period desc limit 4"""
        finance_info_data = pd.read_sql_query(sql_finance_info, self.conn)
        self.df_fi = pd.DataFrame(finance_info_data)  # data-frame finance information
        # print('DF_FI', self.df_fi)

        if resolution == 'D':
            # Load latest price from table tbl_price_board_minute
            sql_latest_price = """select * from tbl_price_board_minute as tpm where tpm.code='""" + code + """' order by t desc limit 1"""
            latest_price = pd.read_sql_query(sql_latest_price, self.conn)
            self.latest_price = pd.DataFrame(latest_price)

        # Load price list
        sql_string = """select * from """ + tbl + """ as pb where pb.code='""" + self.code + """' order by t desc limit 365"""
        # print('sql_string', sql_string)

        sql_query = pd.read_sql_query(sql_string, self.conn)
        df = pd.DataFrame(sql_query)

        if not self.df_fi.empty:
            # finance info #EPS means()
            self.EPS = self.df_fi['eps'].iloc[0]
            self.EPS_MEAN4 = self.df_fi['eps'].mean()
            rev_df_fi = self.df_fi['eps'][::-1]
            self.df_fi['eps_changed'] = rev_df_fi.pct_change()

            print(self.df_fi['eps'])
            print(self.df_fi['eps_changed'].mean())

            self.BVPS = self.df_fi['bvps'].iloc[0]
            self.BVPS_MEAN4 = self.df_fi['bvps'].mean()
            self.PE = self.df_fi['pe'].iloc[0]
            self.PE_MEAN4 = self.df_fi['pe'].mean()
            self.ROS = self.df_fi['ros'].iloc[0]
            self.ROS_MEAN4 = self.df_fi['ros'].mean()
            self.ROEA = self.df_fi['roea'].iloc[0]
            self.ROEA_MEAN4 = self.df_fi['roea'].mean()
            self.ROAA = self.df_fi['roaa'].iloc[0]
            self.ROAA_MEAN4 = self.df_fi['roaa'].mean()
            self.CURRENT_ASSETS = self.df_fi['current_assets'].iloc[0]
            self.CURRENT_ASSETS_MEAN4 = self.df_fi['current_assets'].mean()
            self.TOTAL_ASSETS = self.df_fi['total_assets'].iloc[0]
            self.TOTAL_ASSETS_MEAN4 = self.df_fi['total_assets'].mean()
            self.LIABILITIES = self.df_fi['liabilities'].iloc[0]
            self.LIABILITIES_MEAN4 = self.df_fi['liabilities'].mean()
            self.SHORT_LIABILITIES = self.df_fi['short_term_liabilities'].iloc[0]
            self.SHORT_LIABILITIES_MEAN4 = self.df_fi['short_term_liabilities'].mean()
            self.OWNER_EQUITY = self.df_fi['owner_equity'].iloc[0]
            self.OWNER_EQUITY_MEAN4 = self.df_fi['owner_equity'].mean()
            self.MINORITY_INTEREST = self.df_fi['minority_interest'].iloc[0]
            self.MINORITY_INTEREST_MEAN4 = self.df_fi['minority_interest'].mean()
            self.NET_REVENUE = self.df_fi['net_revenue'].iloc[0]
            self.NET_REVENUE_MEAN4 = self.df_fi['net_revenue'].mean()
            self.GROSS_PROFIT = self.df_fi['gross_profit'].iloc[0]
            self.GROSS_PROFIT_MEAN4 = self.df_fi['gross_profit'].mean()
            self.OPERATING_PROFIT = self.df_fi['operating_profit'].iloc[0]
            self.OPERATING_PROFIT_MEAN4 = self.df_fi['operating_profit'].mean()
            self.PROFIT_AFTER_TAX = self.df_fi['profit_after_tax'].iloc[0]
            self.PROFIT_AFTER_TAX_MEAN4 = self.df_fi['profit_after_tax'].mean()
            self.NET_PROFIT = self.df_fi['net_profit'].iloc[0]
            self.NET_PROFIT_MEAN4 = self.df_fi['net_profit'].mean()
        else:
            # finance info #EPS means()
            self.EPS = 0
            self.EPS_MEAN4 = 0
            rev_df_fi = self.df_fi['eps'][::-1]
            self.df_fi['eps_changed'] = rev_df_fi.pct_change()

            print(self.df_fi['eps'])
            print(self.df_fi['eps_changed'].mean())

            self.BVPS = 0
            self.BVPS_MEAN4 = 0
            self.PE = 0
            self.PE_MEAN4 = 0
            self.ROS = 0
            self.ROS_MEAN4 = 0
            self.ROEA = 0
            self.ROEA_MEAN4 = 0
            self.ROAA = 0
            self.ROAA_MEAN4 = 0
            self.CURRENT_ASSETS = 0
            self.CURRENT_ASSETS_MEAN4 = 0
            self.TOTAL_ASSETS = 0
            self.TOTAL_ASSETS_MEAN4 = 0
            self.LIABILITIES = 0
            self.LIABILITIES_MEAN4 = 0
            self.SHORT_LIABILITIES = 0
            self.SHORT_LIABILITIES_MEAN4 = 0
            self.OWNER_EQUITY = 0
            self.OWNER_EQUITY_MEAN4 = 0
            self.MINORITY_INTEREST = 0
            self.MINORITY_INTEREST_MEAN4 = 0
            self.NET_REVENUE = 0
            self.NET_REVENUE_MEAN4 = 0
            self.GROSS_PROFIT = 0
            self.GROSS_PROFIT_MEAN4 = 0
            self.OPERATING_PROFIT = 0
            self.OPERATING_PROFIT_MEAN4 = 0
            self.PROFIT_AFTER_TAX = 0
            self.PROFIT_AFTER_TAX_MEAN4 = 0
            self.NET_PROFIT = 0
            self.NET_PROFIT_MEAN4 = 0

        # --
        # todo: add current price before reverse string. Current price is from table price_board_minute.
        # todo: Spider price_board_minute runs every minutes
        print('latest_price', self.latest_price)
        # df.append(self.latest_price)

        self.df = df.reindex(index=df.index[::-1]).append(self.latest_price)
        # print('Price List', self.df)
        if not self.df.empty:
            self.LAST_SESSION = self.df['t'].iloc[-1]
            self.df['changed'] = self.df['c'].pct_change()
            self.df['rsi'] = talib.RSI(self.df['c'])
            self.df['cci'] = talib.CCI(self.df['h'], self.df['l'], self.df['c'], timeperiod=20)

            self.df['macd'], self.df['macdsignal'], self.df['macdhist'] = talib.MACD(self.df['c'], fastperiod=12,
                                                                                         slowperiod=26, signalperiod=9)

            self.CURRENT_CLOSE = self.df['c'].iloc[-1]
            self.df['SMA_5'] = talib.SMA(self.df['c'], timeperiod=5)
            self.df['SMA_10'] = talib.SMA(self.df['c'], timeperiod=10)
            self.df['SMA_20'] = talib.SMA(self.df['c'], timeperiod=20)
            self.df['SMA_50'] = talib.SMA(self.df['c'], timeperiod=50)
            self.df['SMA_150'] = talib.SMA(self.df['c'], timeperiod=150)
            self.df['SMA_200'] = talib.SMA(self.df['c'], timeperiod=200)
            # Volume SMA 20
            self.df['V_SMA_20'] = talib.SMA(self.df['v'], timeperiod=20)
            self.LAST_V_SMA_20 = self.df['V_SMA_20'].iloc[-1]

            values = []
            for index, cci in self.df['cci'].items():
                v = 0
                if index >= (len(df) - 1) or index <= 0:
                    v = 0
                else:
                    if (cci < self.df['cci'].iloc[index + 1]) and (cci < self.df['cci'].iloc[index - 1]):
                        v = -1
                    elif (cci > self.df['cci'].iloc[index + 1]) and (cci > self.df['cci'].iloc[index - 1]):
                        v = 1
                    else:
                        v = 0
                values.append(v)
            self.df['cci_curve'] = pd.Series(values)

        # 4 weeks
        try:
            self.SMA_200_20 = talib.SMA(self.df['close'], timeperiod=200).iloc[-20]
        except:
            self.SMA_200_20 = 0

        self.LOW_52W = self.df['l'].head(260).min()
        self.HIGH_52W = self.df['h'].head(260).max()

    def f_get_current_price(self):
        return self.df['c'].iloc[-1]

    def f_check_has_value(self):
        return not self.df.empty

    # Kiem tra gia tri giao dich trong phien. Toi thieu 3ty, recommend 5ty, best >10ty
    def f_check_gia_tri_giao_dich_trong_phien(self, value=5000000000.0):
        return (not self.df.empty and self.LAST_V_SMA_20 * self.CURRENT_CLOSE) >= value

    # last changed
    def f_last_changed(self):
        return self.df['changed'].iloc[-1]

    # Check gia chua tang qua 3 phien lien tiep
    def f_check_price_continous_jump(self, step=3):
        return not (self.df['c'].iloc[-1] > self.df['c'].iloc[-2] > self.df['c'].iloc[-3])

    def f_1stRSI(self):
        try:
            return self.df['rsi'].iloc[-1]
        except:
            return 0

    def f_2ndRSI(self):
        try:
            return self.df['rsi'].iloc[-2]
        except:
            return 0

    def f_3rdRSI(self):
        try:
            return self.df['rsi'].iloc[-3]
        except:
            return 0

    def f_1stCCI(self):
        return self.df['cci'].iloc[-1]

    def f_cci_3_days_down(self):
        return self.df['cci'].iloc[-1] < self.df['cci'].iloc[-2] < self.df['cci'].iloc[-3]

    def f_2ndCCI(self):
        return self.df['cci'].iloc[-2]

    def f_3rdCCI(self):
        return self.df['cci'].iloc[-3]

    # upper, middle, lower
    def f_bband(self):
        # close, matype = MA_Type.T3
        return talib.BBANDS(self.df['c'], timeperiod=5, matype=talib.MA_Type.T3)

    # macd, macdsignal, macdhist = MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    def f_macd(self):
        return talib.MACD(self.df['c'], fastperiod=12, slowperiod=26, signalperiod=9)

    # fastk, fastd = STOCHRSI(close, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
    def f_stochrsi(self):
        return talib.STOCHRSI(self.df['c'], timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)

    def f_total_vol(self):
        return self.df['v'].iloc[-1]

    # check if current is top/bottom
    def f_is_current_possible_top(self, window=3):
        if hasattr(self.df, 'cci'):
            windowCCI = self.df['cci'].tail(window)
            max = windowCCI.max()
            # print('windowCCI', windowCCI)
            return (max > windowCCI.iloc[0]) and (max > windowCCI.iloc[-1])
        else:
            return False

    def f_is_current_possible_bottom(self, window=3):
        if hasattr(self.df, 'cci'):
            windowCCI = self.df['cci'].tail(window)
            min = windowCCI.min()
            # print('windowCCI', windowCCI)
            return (min < windowCCI.iloc[0]) and (min < windowCCI.iloc[-1])
        else:
            return False

    # Oversold --> should buy
    def f_is_over_sold(self, horizontal=100):
        return self.cci.iloc[-1] <= (0 - horizontal)

    # Overbought --> should sell
    def f_is_over_bought(self, horizontal=100):
        return self.cci.iloc[-1] > horizontal

    def f_check_uptrend_1_month(self):
        return self.CURRENT_CLOSE > self.df['SMA_5'].iloc[-1] > self.df['SMA_10'].iloc[-1] > self.df['SMA_20'].iloc[-1]

    # check price jump 1%
    def f_check_price_jump(self, step=0.01):
        return self.df['changed'].iloc[-1] >= step

    # minervini conditions
    def f_check_7_conditions(self):
        # Condition 1: Current Price > 150 SMA and > 200 SMA
        condition_1 = self.CURRENT_CLOSE > self.df['SMA_150'].iloc[-1] > self.df['SMA_200'].iloc[-1]

        # Condition 2: 150 SMA and > 200 SMA
        condition_2 = self.df['SMA_150'].iloc[-1] > self.df['SMA_200'].iloc[-1]

        # Condition 3: 200 SMA trending up for at least 1 month
        condition_3 = self.df['SMA_200'].iloc[-1] > self.SMA_200_20

        # Condition 4: 50 SMA> 150 SMA and 50 SMA> 200 SMA
        condition_4 = self.df['SMA_50'].iloc[-1] > self.df['SMA_150'].iloc[-1] > self.df['SMA_200'].iloc[-1]

        # Condition 5: Current Price > 50 SMA
        condition_5 = self.CURRENT_CLOSE > self.df['SMA_50'].iloc[-1]

        # Condition 6: Current Price is at least 30% above 52 week low
        condition_6 = self.CURRENT_CLOSE >= (1.3 * self.LOW_52W)

        # Condition 7: Current Price is within 25% of 52 week high
        condition_7 = self.CURRENT_CLOSE >= (.75 * self.HIGH_52W)

        return condition_1 and \
               condition_2 and \
               condition_3 and \
               condition_4 and \
               condition_5 and \
               condition_6 and \
               condition_7

    # destructor
    def __del__(self):
        self.conn.close()
