import pymysql
import pandas as pd

from stock import Stock

if __name__=='__main__':
    print('Hello ...')
    conn = pymysql.connect(host='localhost', user='admin', password='123456', database='mystocks')
    cursor = conn.cursor()
    # sql_query = pd.read_sql_query('''select * from price_board as pb where pb.t_code="FPT"''', conn)
    sql_query = pd.read_sql_query('''select * from tbl_company ''', conn)
    #
    df = pd.DataFrame(sql_query)
    # print('df', df)
    # df['stock'] = Stock(df['code'])
    # for (i, code, stock) in df.iterrows():
    #     print(code, stock)
    # _session = datetime.now().date()
    # s = Stock('FPT')
    # isTop = s.f_is_current_possible_top(window=3)
    # isBot = s.f_is_current_possible_bottom(window=3)
    # print('isTop', isTop)
    # print('isBot', isBot)

    rows = []
    for idx, code in df['Code'].iteritems():
        s = Stock(code)
        # if s.f_check_7_conditions():
        #     print('Good code: ', code)

        if s.f_check_has_value() and s.f_check_gia_tri_giao_dich_trong_phien() and s.f_check_uptrend_1_month() and s.f_check_price_jump() and s.f_check_price_continous_jump():
            print(s.LAST_SESSION, "Good to buy: ", code, s.f_total_vol(), "last CCI: ", s.f_1stCCI())
            rows.append([s.LAST_SESSION, code, s.f_total_vol(), s.EPS, s.EPS_MEAN4, s.f_1stCCI(), s.f_last_changed()])

        # if s.f_is_current_possible_bottom() and s.f_1stCCI() < -100 and s.f_check_gia_tri_giao_dich_trong_phien():
        #     rows.append([s.LAST_SESSION, code, s.f_total_vol(), s.EPS, s.EPS_MEAN4, s.f_1stCCI(), s.f_last_changed()])
        # del s

    results = pd.DataFrame(rows, columns=["Session", "Code", "Volume", "EPS", "EPS_MEAN4", "CCI", 'Changed'])
    results.to_excel("outputs/output_cci_100.xlsx", sheet_name="GoodCodes")


# upper, middle, lower = s.f_bband()
# macd, macdsignal, macdhist = s.f_macd()
# fastk, fastd = s.f_stochrsi()
#
# print('s.f_total_vol()', s.f_total_vol())
#
# # tuple1 = (
# #     code,
# #     _session,
# #     s.f_1stRSI(),
# #     s.f_2ndRSI(),
# #     s.f_3rdRSI(),
# #     s.f_1stCCI(),
# #     s.f_2ndCCI(),
# #     s.f_3rdCCI(),
# #     upper,
# #     middle,
# #     lower,
# #     macd,
# #     macdsignal,
# #     macdhist,
# #     fastk,
# #     fastd,
# #     s.f_vol1(),
# #     s.f_vol2(),
# #     s.f_total_vol()
# # )
# # print(tuple1)
# # insert into database
#
# sql_str = """INSERT INTO processed_data(code, session, rsi1, rsi2, rsi3, cci1, cci2, cci3, bbmax, bbmid, bbmin, macd, macd_signal, macd_histogram, fastk, fastd, vol1, vol2, total_vol) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
# try:
#     cursor.execute(sql_str, (
#         code,
#         _session,
#         s.f_1stRSI(),
#         s.f_2ndRSI(),
#         s.f_3rdRSI(),
#         s.f_1stCCI(),
#         s.f_2ndCCI(),
#         s.f_3rdCCI(),
#         upper.iloc[-1],
#         middle.iloc[-1],
#         lower.iloc[-1],
#         macd.iloc[-1],
#         macdsignal.iloc[-1],
#         macdhist.iloc[-1],
#         fastk.iloc[-1],
#         fastd.iloc[-1],
#         s.f_vol1(),
#         s.f_vol2(),
#         0
#     ))
#     conn.commit()
# except:
#     continue