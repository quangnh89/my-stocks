import pymysql
import scrapy
import json
import pandas as pd
import re
from datetime import datetime

from itemloaders import ItemLoader

from StockSpiders3.items import OHLC
from StockSpiders3.utils import Config


class VietstockpricesboardSpider(scrapy.Spider):
    name = 'vietstockpricesboard'
    allowed_domains = ['vietstock.vn']
    start_urls = ['https://finance.vietstock.vn']
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'}
    api_url = "https://api.vietstock.vn/ta/history"
    # 1420045200 - 2015/1/1 00:00:00
    START_DATE = 1420045200
    DAILY = False
    df_companies = None
    companies = None

    def __init__(self, resolution='D', **kwargs):
        self.resolution = resolution
        self.config = Config()
        super().__init__(**kwargs)

    # https://api.vietstock.vn/ta/history?symbol=BHP&resolution=D&from=1586252875&to=1617356935

    def start_requests(self):
        self.DAILY = self.settings.get('RUN_DAILY', False)
        db_settings = self.settings.getdict("DB_SETTINGS")
        db = db_settings['db']
        user = db_settings['user']
        passwd = db_settings['passwd']
        host = db_settings['host']

        conn = pymysql.connect(host=host, user=user, password=passwd, database=db,
                               cursorclass=pymysql.cursors.DictCursor)
        data = pd.read_sql_query('''select * from tbl_company ''', conn)

        self.df_companies = pd.DataFrame(data)
        self.companies = self.df_companies['Code']

        for url in self.start_urls:
            yield scrapy.Request(url, headers=self.headers, callback=self.login)

    def login(self, response):
        request_verification_token = response.xpath('//*[@id="form1"]/input/@value').extract_first()
        yield scrapy.FormRequest.from_response(
            response,
            formid='form1',
            formdata={
                '__RequestVerificationToken': request_verification_token,
                'Email': 'vubakninh@gmail.com',
                'Password': '6GGRrGbq@D6FNk5',
                'responseCaptchaLoginPopup': '',
                'g-recaptcha-response': '',
                'X-Requested-With': 'XMLHttpRequest',
                'Remember': 'false'
            },
            clickdata={'id': 'btnLoginAccount'},
            headers=self.headers,
            callback=self.after_login
        )

    def after_login(self, response):
        # check resolution
        ts1 = int(datetime.now().timestamp())
        if (self.resolution == 'D') and (ts1 > (self.config.f_last_run_day() + 86400)):
            ts2 = self.config.f_last_run_day()
            step = 240 * 24 * 60 * 60
        elif (self.resolution == '1') and (ts1 > (self.config.f_last_run_minute() + 60)):
            ts2 = self.config.f_last_run_minute()
            step = 240 * 60
        elif self.resolution == '60' and (ts1 > (self.config.f_last_run_hour() + 3600)):
            ts2 = self.config.f_last_run_hour()
            step = 240 * 60 * 60
        else:
            ts2 = ts1

        print('Resolution', self.resolution)
        print('last_run', self.config.f_last_run_day())
        #
        # self.config.f_update_last_run('D')
        # pass

        for code in self.companies:
            # print('code', code, ts2, ts1)
            for x in range(ts2, ts1, step):
                ts3 = ts1 if (x + step > ts1) else (x + step)
                # print('Ts3', ts3)
                url = self.api_url + '?symbol=' + code + '&resolution=' + self.resolution + '&from=' + str(x) + '&to=' + str(ts3)
                # print('URL', url)
                yield scrapy.Request(url=url, headers=self.headers, callback=self.parse, meta={'code': code})

            # while ts2 >= self.START_DATE:
            #     ts2 = ts1 - 20000000
            #     url = self.api_url + '?symbol=' + code + '&resolution=D&from=' + str(ts2) + '&to=' + str(ts1)
            #     ts1 = ts2
            #     yield scrapy.Request(url=url, headers=self.headers, callback=self.parse, meta={'code': code})

    def parse(self, response):
        code = response.meta.get('code')
        data = re.sub('<[^<]+>', "", response.text)
        data = json.loads(data)
        results = []
        for idx in range(len(data['t'])):
            item = OHLC()
            item['res'] = self.resolution
            item['code'] = code
            item['t'] = data['t'][idx]
            item['o'] = data['o'][idx]
            item['h'] = data['h'][idx]
            item['l'] = data['l'][idx]
            item['c'] = data['c'][idx]
            item['v'] = data['v'][idx]
            results.append(item)
        return results

        # print(response.text)
        # print(response.body_as_unicode())
        # pass

    def closed(self, reason):
        self.config.f_update_last_run(resolution=self.resolution)
