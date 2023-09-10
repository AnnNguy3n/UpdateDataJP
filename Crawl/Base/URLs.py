from __init__ import ROOT_PATH


class IrBank:
    PATH_LIST_SECTOR = ROOT_PATH + "\\Crawl\\Base\\List_sector.csv"
    LIST_COMPANY = "https://irbank.net/category/__SECTOR__"
    FINANCIAL_REPORT = "https://irbank.net/__CODE__/reports"


class MorningStar:
    FINANCIAL_REPORT = "https://www.morningstar.com/stocks/xtks/__SYMBOL__/financials"
    VOLUME = "https://www.morningstar.com/stocks/xtks/__SYMBOL__/quote"


class YahooJP:
    PRICE_CLOSED = "https://finance.yahoo.co.jp/quote/__SYMBOL__.T/history?from=__START_DATE__&to=__END_DATE__&timeFrame=d&page=__PAGE__"


class Kabu:
    TABLE_DIVIDEND_1 = "https://kabu.com/investment/meigara/bunkatu.html"
    TABLE_DIVIDEND_2 = "https://kabu.com/investment/meigara/gensi.html"


class Buffet:
    VOLUME = "https://www.buffett-code.com/company/__SYMBOL__/"
