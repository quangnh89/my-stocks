import re
from datetime import datetime

import pymysql
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from StockSpiders3.items import StocksItem


def strip(text):
    return text.replace(u'\xa0', u' ').strip().replace(',', '')


class BaseDBPipeline:
    def __init__(self, db, user, passwd, host):
        self.db = db
        self.user = user
        self.passwd = passwd
        self.host = host
        self.conn = None
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.passwd, database=self.db,
                                    cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.conn.cursor()
        # print('database', self.db)
        # print('user', self.user)
        # print('passwd', self.passwd)
        # print('host', self.host)

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


class InitDatabasePipeline(BaseDBPipeline):
    def open_spider(self, spider):
        # create table if not exists
        tbl_stock = '''create table if not exists stock(
            code varchar(32) not null primary key,
            company_name varchar(512) null,
            warning_state tinyint(1) default 0 null
            );'''

        tbl_company = '''create table if not exists tbl_company(
            Code varchar(32) not null primary key,
            CatID int,
            Exchange varchar(256),
            ID int,
            IndustryName varchar(256),
            Name varchar(256),
            TotalShares int,
            URL varchar(512)
        );'''

        try:
            self.cursor.execute(tbl_stock)
            self.cursor.execute(tbl_company)
        except:
            raise DropItem('Cannot init database')


class VietstockPriceboardPipeline(BaseDBPipeline):
    def process_item(self, item, spider):
        if spider.name == 'vietstockpricesboard':
            self.insert_vietstock_price_board_day(item)
        else:
            return item

    def insert_vietstock_price_board_day(self, item):
        sql_statement = '''INSERT INTO tbl_price_board_day(code, t, o, h, l, c, v) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        adt = ItemAdapter(item)
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


class VietstockPriceboardHourPipeline(BaseDBPipeline):
    def process_item(self, item, spider):
        if spider.name == 'vietstockpricesboard_hour':
            self.insert_vietstock_price_board_day(item)
        else:
            return item

    def insert_vietstock_price_board_day(self, item):
        sql_statement = '''INSERT INTO tbl_price_board_hour(code, t, o, h, l, c, v) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        adt = ItemAdapter(item)
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


class VietstockPriceboardMinutePipeline(BaseDBPipeline):
    def process_item(self, item, spider):
        if spider.name == 'vietstockpricesboard_minute':
            self.insert_vietstock_price_board_day(item)
        else:
            return item

    def insert_vietstock_price_board_day(self, item):
        sql_statement = '''INSERT INTO tbl_price_board_minute(code, t, o, h, l, c, v) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        adt = ItemAdapter(item)
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
        print('Processing FINANCE INFO')
        if spider.name == 'vietstockfinanceinfo':
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


class DatabasePipeline(BaseDBPipeline):
    def open_spider(self, spider):

        if spider.name == 'hsx':
            self.load_all_stock()
        elif spider.name == 'bctc':
            print('bctc^')

    def process_item(self, item, spider):
        if spider.name == 'hsx':
            self.insert_new_stock_code(item)
            self.insert_price_board(item)
        else:
            return item

    def load_all_stock(self):
        sql = 'SELECT * FROM stock'
        try:
            self.cursor.execute(sql)
            self.all_stock = self.cursor.fetchall()

            # print('All Stocks', self.all_stock)
        except:
            self.all_stock = []

    def is_code_exists(self, code):
        return any(x['code'] == code for x in self.all_stock)

    def insert_price_board(self, item):
        sql = '''INSERT INTO price_board(t_session, t_code, t_close, t_changed, t_changed_percentage, t_ref, t_open, t_high, t_low, t_vol1, t_val1, t_vol2, t_val2) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        adapter = ItemAdapter(item)

        try:
            self.cursor.execute(sql, (
                adapter.get('session'),
                adapter.get('code'),
                float(adapter.get('close')),
                float(adapter.get('changed')),
                float(adapter.get('changed_percent')),
                float(adapter.get('reference')),
                float(adapter.get('open')),
                float(adapter.get('high')),
                float(adapter.get('low')),
                int(adapter.get('klkl')),
                int(adapter.get('gtkl')),
                int(adapter.get('kltt')),
                int(adapter.get('gttt')),
            ))
            self.conn.commit()
        except:
            raise DropItem(f"Missing price in {item}")
        return item

    def insert_new_stock_code(self, item):
        adapter = ItemAdapter(item)
        if not self.is_code_exists(adapter.get('code')):
            sql = 'INSERT INTO stock(code, company_name, warning_state) VALUES (%s, %s, %s)'
            try:
                self.cursor.execute(sql, (adapter.get('code'), '', False))
                self.conn.commit()
                self.all_stock.append({'code': adapter.get('code'), 'company_name': '', 'warning_state': 0})
            except:
                pass

        return item

    def close_spider(self, spider):
        if self.conn is not None and self.conn.open == 1:
            self.conn.close()


class HtmlCleanerPipeline:
    def process_item(self, item, spider):
        if spider.name == 'hsx':
            adapter = ItemAdapter(item)
            session = adapter.get('session')

            match1 = re.search(r'\d{2}\/\d{2}\/\d{4}', session)
            date = datetime.strptime(match1.group(), '%d/%m/%Y').date()

            # -4.70 (-7.0 %)
            x_changed = strip(adapter.get('changed')).split()

            xc1 = x_changed[0]
            xc2 = x_changed[1][1:]

            item2 = StocksItem()
            item2['session'] = date
            item2['code'] = strip(adapter.get('code'))
            item2['close'] = strip(adapter.get('close'))
            item2['changed'] = xc1
            item2['changed_percent'] = xc2
            item2['reference'] = strip(adapter.get('reference'))
            item2['open'] = strip(adapter.get('open'))
            item2['high'] = strip(adapter.get('high'))
            item2['low'] = strip(adapter.get('low'))
            item2['klkl'] = strip(adapter.get('klkl'))
            item2['gtkl'] = strip(adapter.get('gtkl'))
            item2['kltt'] = strip(adapter.get('kltt'))
            item2['gttt'] = strip(adapter.get('gttt'))
            return item2
        else:
            return item
