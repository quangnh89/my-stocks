import json

import scrapy
from itemloaders import ItemLoader
from itemloaders.processors import MapCompose, Join, SelectJmes

from StockSpiders3.items import CompanyItem


def authentication_failed(response):
    # TODO: Check the contents of the response and return True if it failed
    # or False if it succeeded.
    pass


class VietstockazSpider(scrapy.Spider):
    name = 'vietstockaz'
    allowed_domains = ['vietstock.vn']
    start_urls = ['https://vietstock.vn/']
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'}
    my_paths = {
        'CatID': 'CatID',
        'Code': 'Code',
        'Exchange': 'Exchange',
        'ID': 'ID',
        'IndustryName': 'IndustryName',
        'Name': 'Name',
        'TotalShares': 'TotalShares',
        'URL': 'URL'
    }

    def start_requests(self):
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
        if authentication_failed(response):
            self.logger.error("Login failed")
            return
        else:
            fdata = {
                'catID': '0',
                'industryID': '0',
                'page': '0',
                'pageSize': '50',
                'code': '',
                'type': '0',
                'businessTypeID': '0',
                'orderBy': 'Code',
                'orderDir': 'ASC'
            }

            url = 'https://finance.vietstock.vn/data/corporateaz'
            for i in range(0, 65, 1):
                fdata['page'] = str(i)
                # print(fdata)
            # pass
                yield scrapy.FormRequest(url, formdata=fdata,
                                         headers=self.headers, callback=self.parse)

    def parse(self, response):
        # open_in_browser(response)
        jsonresponse = json.loads(response.body_as_unicode())
        data = []
        for it in jsonresponse:
            item = CompanyItem()
            item['Code'] = it['Code']
            item['CatID'] = it['CatID']
            item['Exchange'] = it['Exchange']
            item['ID'] = it['ID']
            item['IndustryName'] = it['IndustryName']
            item['Name'] = it['Name']
            item['URL'] = it['URL']

            data.append(item)
            # loader = ItemLoader(item=CompanyItem())
            # loader.default_input_processor = MapCompose(str)  # apply str conversion on each value
            # loader.default_output_processor = Join(' ')
            # for (field, path) in self.my_paths.items():
            #     loader.add_value(field, SelectJmes(path)(it))
            # data.append(loader.load_item())
        return data
