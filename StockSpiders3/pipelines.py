import re
import os
from datetime import datetime

import pymysql
from dotenv import load_dotenv
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

def strip(text):
    return text.replace(u'\xa0', u' ').strip().replace(',', '')


class BaseDBPipeline:
    def __init__(self, db, user, passwd, host):
        self.db = db
        self.user = user
        self.passwd = passwd
        self.host = host
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.passwd, database=self.db,
                                    cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.conn.cursor()

    @classmethod
    def from_crawler(cls, crawler):
        db_settings = crawler.settings.getdict("DB_SETTINGS")
        if not db_settings:
            raise Exception
        db = db_settings['db']
        user = db_settings['user']
        passwd = db_settings['passwd']
        host = db_settings['host']
        return cls(db, user, passwd, host)  # returning pipeline instance


class VietstockPriceboardPipeline(BaseDBPipeline):
    def process_item(self, item, spider):
        if spider.name == 'vietstockpricesboard':
            self.insert_vietstock_price_board_day(item)
        else:
            return item

    def insert_vietstock_price_board_day(self, item):
        adt = ItemAdapter(item)
        if adt.get('res') == 'D':
            tbl = 'tbl_price_board_day'
        elif adt.get('res') == '1':
            tbl = 'tbl_price_board_minute'
        elif adt.get('res') == '60':
            tbl = 'tbl_price_board_hour'

        sql_statement = '''INSERT INTO ''' + tbl + '''(code, t, o, h, l, c, v) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        try:
            self.cursor.execute(sql_statement, (
                adt.get('code'),
                adt.get('t'),
                adt.get('o'),
                adt.get('h'),
                adt.get('l'),
                adt.get('c'),
                adt.get('v')
            ))
        except:
            raise DropItem("Error")
        finally:
            self.conn.commit()
        return item


class VietstockCompanyAZPipeline(BaseDBPipeline):
    def process_item(self, item, spider):
        if spider.name == 'vietstockaz':
            self.insert_new_company(item)
        else:
            return item

    def insert_new_company(self, item):
        sql_statement = '''INSERT INTO tbl_company(CatID, Code, Exchange, ID, IndustryName, Name, TotalShares, URL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
        adt = ItemAdapter(item)
        try:
            self.cursor.execute(sql_statement, (
                adt.get('CatID'),
                adt.get('Code'),
                adt.get('Exchange'),
                adt.get('ID'),
                adt.get('IndustryName'),
                adt.get('Name'),
                adt.get('TotalShares'),
                adt.get('URL')
            ))
        except:
            raise DropItem('Error')
        finally:
            self.conn.commit()


class VietstockFinanceInfoPipeline(BaseDBPipeline):
    def process_item(self, item, spider):
        if spider.name == 'vietstockfinanceinfo':
            print('Processing FINANCE INFO')
            self.insert_finance_info(item)
        else:
            return item

    def insert_finance_info(self, item):
        sql_statement = '''INSERT INTO tbl_finance_info(
        company_id,
        year_period,
        quarter_period,
        audited_status,
        code,
        eps,
        bvps,
        pe,
        ros,
        roea,
        roaa,
        current_assets,
        total_assets,
        liabilities,
        short_term_liabilities,
        owner_equity,
        minority_interest,
        net_revenue,
        gross_profit,
        operating_profit,
        profit_after_tax,
        net_profit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        adt = ItemAdapter(item)
        try:
            self.cursor.execute(sql_statement, (
                adt.get('companyId'),
                adt.get('yearPeriod'),
                adt.get('quarterPeriod'),
                adt.get('auditedStatus'),
                adt.get('code'),
                adt.get('eps'),
                adt.get('bvps'),
                adt.get('pe'),
                adt.get('ros'),
                adt.get('roea'),
                adt.get('roaa'),
                adt.get('currentAssets'),
                adt.get('totalAssets'),
                adt.get('liabilities'),
                adt.get('shortTermLiabilities'),
                adt.get('ownerEquity'),
                adt.get('minorityInterest'),
                adt.get('netRevenue'),
                adt.get('grossProfit'),
                adt.get('operatingProfit'),
                adt.get('profitAfterTax'),
                adt.get('netProfit')
            ))

        except:
            raise DropItem('Error')
        finally:
            self.conn.commit()
