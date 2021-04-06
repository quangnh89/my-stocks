import scrapy


class VietstockpricesboardHourSpider(scrapy.Spider):
    name = 'vietstockpricesboard_hour'
    allowed_domains = ['finance.vietstock.vn']
    start_urls = ['http://finance.vietstock.vn/']

    def parse(self, response):
        pass
