from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

from StockSpiders3.spiders.vietstockpricesboard import VietstockpricesboardSpider

settings = get_project_settings()

settings.update({
    "LOG_ENABLED": "False"
})

configure_logging()
runner = CrawlerRunner(settings)


@defer.inlineCallbacks
def crawl():
    yield runner.crawl(VietstockpricesboardSpider, resolution='D')
    yield runner.crawl(VietstockpricesboardSpider, resolution='1')
    reactor.stop()


crawl()
reactor.run()  # the script will block here until the last crawl call is finished
