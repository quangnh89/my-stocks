# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field


class StocksItem(scrapy.Item):
    session = Field()
    code = Field()
    close = Field()
    changed = Field()
    changed_percent = Field()
    reference = Field()
    open = Field()
    high = Field()
    low = Field()
    klkl = Field()
    gtkl = Field()
    kltt = Field()
    gttt = Field()


class OHLC(scrapy.Item):
    code = Field()
    t = Field()
    o = Field()
    h = Field()
    l = Field()
    c = Field()
    v = Field()


class CompanyItem(scrapy.Item):
    CatID = Field()
    Code = Field()
    Exchange = Field()
    ID = Field()
    IndustryName = Field()
    Name = Field()
    TotalShares = Field()
    URL = Field()


class FinanceInfo(scrapy.Item):
    companyId = Field()
    yearPeriod = Field()
    quarterPeriod = Field()
    auditedStatus = Field()
    code = Field()
    eps = Field()
    bvps = Field()
    pe = Field()
    ros = Field()
    roea = Field()
    roaa = Field()
    # Can doi ke toan
    currentAssets = Field()  # Tai san ngan han
    totalAssets = Field()
    liabilities = Field()  # No phai tra
    shortTermLiabilities = Field()  # No ngan han phai tra
    ownerEquity = Field()  # Von chu so huu
    minorityInterest = Field()  # Loi ich co dong thieu so

    #  Ket qua kinh doanh
    netRevenue = Field()  # Doanh thu thuan
    grossProfit = Field()  # Loi nhuan gop
    operatingProfit = Field()  # Loi nhuan thuan tu hoat dong kinh doanh
    profitAfterTax = Field()  # Loi nhuan sau thue thu nhap doanh nghiep
    netProfit = Field()  #  Loi nhuan sau thue cua CD cong ty me
