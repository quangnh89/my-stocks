import pandas as pd
import pymysql
import talib


class Stock:
    def __init__(self, code, resolution='D'):
        self.code = code
        self.resolution = resolution

        # Load finance info
        """select * from tbl_finance_info as ti where ti.code='"""+code+"""' order by year_period desc, quarter_period desc limit 4"""

        # Load prices
        if resolution == 'D':
            tbl = 'tbl_price_board_day'
        elif resolution == 'H':
            tbl = 'tbl_price_board_hour'
        elif resolution == 'M':
            tbl = 'tbl_price_board_minute'
        else:
            tbl = 'tbl_price_board_day'

        self.conn = pymysql.connect(host='localhost', user='admin', password='123456', database='mystocks')

        sql_string = """select * from """ + tbl + """ as pb where pb.code='""" + self.code + """' order by t desc limit 365"""
        # print('sql_string', sql_string)

        sql_query = pd.read_sql_query(sql_string, self.conn)
        df = pd.DataFrame(sql_query)

        # todo: add current price before reverse string. Current price is from table price_board_minute.
        # todo: Spider price_board_minute runs every minutes

        self.df = df.reindex(index=df.index[::-1])
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
        windowCCI = self.df['cci'].tail(window)
        max = windowCCI.max()
        print('windowCCI', windowCCI)
        return (max > windowCCI.iloc[0]) and (max > windowCCI.iloc[-1])

    def f_is_current_possible_bottom(self, window=3):
        windowCCI = self.df['cci'].tail(window)
        min = windowCCI.min()
        # print('windowCCI', windowCCI)
        return (min < windowCCI.iloc[0]) and (min < windowCCI.iloc[-1])

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
