import scrapy


class VietstockpricesboardMinuteSpider(scrapy.Spider):
    name = 'vietstockpricesboard_minute'
    allowed_domains = ['finance.vietstock.vn']
    start_urls = ['http://finance.vietstock.vn/']

    def parse(self, response):
        pass
