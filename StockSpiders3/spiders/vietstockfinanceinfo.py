import pymysql
import scrapy
import json
import pandas as pd
import re
from datetime import datetime
from scrapy.utils.response import open_in_browser

from StockSpiders3.items import FinanceInfo


class VietstockfinanceinfoSpider(scrapy.Spider):
    name = 'vietstockfinanceinfo'
    allowed_domains = ['finance.vietstock.vn']
    start_urls = ['https://finance.vietstock.vn/']
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'}

    def start_requests(self):
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
        # open_in_browser(response)
        for row in self.df_companies.itertuples(index=True):
            url = getattr(row, 'URL')
            code = getattr(row, 'Code')
            id = getattr(row, 'ID')
            yield scrapy.Request(url, headers=self.headers, callback=self.load_company_page, meta={'code': code, 'id': id})
        # row = self.df_companies.iloc[100]
        # url = getattr(row, 'URL')
        # code = getattr(row, 'Code') + '?tab=BCTQ'
        # id = getattr(row, 'ID')
        # print(row)
        # yield scrapy.Request(url, headers=self.headers, callback=self.load_company_page, meta={'code': code, 'id': id})

    def load_company_page(self, response):
        request_verification_token = response.xpath('//*[@id="__CHART_AjaxAntiForgeryForm"]/input/@value').extract_first()
        code = response.meta.get('code')
        url = 'https://finance.vietstock.vn/data/financeinfo'
        fdata = {
            'Code': code,
            'Page': '1',
            'PageSize': '4',
            'ReportTermType': '2',
            'ReportType': 'BCTQ',
            'Unit': '1000000',
            '__RequestVerificationToken': request_verification_token
        }
        yield scrapy.FormRequest(
            url=url,
            formdata=fdata,
            headers=self.headers,
            callback=self.parse,
            meta={'code': code}
        )

    def parse(self, response):
        # open_in_browser(response)
        code = response.meta.get('code')
        jobject = json.loads(response.text)
        l = len(jobject[0])
        print('Parsing ... ', l, code)
        results = []
        for idx in range(l):
            item = FinanceInfo()
            # row 1
            item['code'] = code
            item['companyId'] = jobject[0][idx]['CompanyID']
            item['yearPeriod'] = jobject[0][idx]['YearPeriod']
            item['quarterPeriod'] = jobject[0][idx]['TermCode']
            item['auditedStatus'] = jobject[0][idx]['AuditedStatus']
            # row 2#EPS
            item['eps'] = jobject[1]['Chỉ số tài chính'][0].get('Value' + str(l-idx), 0)
            # row 2#BVPS
            item['bvps'] = jobject[1]['Chỉ số tài chính'][1].get('Value' + str(l-idx), 0)
            # row 2#PE
            item['pe'] = jobject[1]['Chỉ số tài chính'][2].get('Value' + str(l-idx), 0)
            # row 2#ROS
            item['ros'] = jobject[1]['Chỉ số tài chính'][3].get('Value' + str(l-idx), 0)
            # row 2#ROEA
            item['roea'] = jobject[1]['Chỉ số tài chính'][4].get('Value' + str(l-idx), 0)
            # row 2#ROAA
            item['roaa'] = jobject[1]['Chỉ số tài chính'][5].get('Value' + str(l-idx), 0)
            # Cân đối kế toán # Tai san ngan han
            item['currentAssets'] = jobject[1]['Cân đối kế toán'][0].get('Value' + str(l-idx), 0)
            # Cân đối kế toán # tong tai san
            item['totalAssets'] = jobject[1]['Cân đối kế toán'][1].get('Value' + str(l-idx), 0)
            # Cân đối kế toán # No phai tra
            item['liabilities'] = jobject[1]['Cân đối kế toán'][2].get('Value' + str(l-idx), 0)
            # Cân đối kế toán # No ngan han phai tra
            item['shortTermLiabilities'] = jobject[1]['Cân đối kế toán'][3].get('Value' + str(l-idx), 0)
            # Cân đối kế toán # Von chu so huu
            item['ownerEquity'] = jobject[1]['Cân đối kế toán'][4].get('Value' + str(l-idx), 0)
            # Cân đối kế toán # Loi ich co dong thieu so
            item['minorityInterest'] = jobject[1]['Cân đối kế toán'][5].get('Value' + str(l-idx), 0)
            # Kết quả kinh doanh # Doanh thu thuan
            item['netRevenue'] = jobject[1]['Kết quả kinh doanh'][0].get('Value' + str(l-idx), 0)
            # Kết quả kinh doanh # Loi nhuan gop
            item['grossProfit'] = jobject[1]['Kết quả kinh doanh'][1].get('Value' + str(l-idx), 0)
            # Kết quả kinh doanh # Loi nhuan thuan tu hoat dong kinh doanh
            item['operatingProfit'] = jobject[1]['Kết quả kinh doanh'][2].get('Value' + str(l-idx), 0)
            # Kết quả kinh doanh # Loi nhuan sau thue thu nhap doanh nghiep
            item['profitAfterTax'] = jobject[1]['Kết quả kinh doanh'][3].get('Value' + str(l-idx), 0)
            # Kết quả kinh doanh # Loi nhuan sau thue cua CD cong ty me
            item['netProfit'] = jobject[1]['Kết quả kinh doanh'][4].get('Value' + str(l-idx), 0)
            results.append(item)
        return results
