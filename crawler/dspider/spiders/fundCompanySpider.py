# -*- coding: utf-8 -*-
import const as ct
import os, re, json, csv 
from lxml import etree
from datetime import datetime
from base.clog import getLogger 
from ccalendar import CCalendar
from scrapy import signals, Request
from dspider.myspider import BasicSpider
class FundCompanySpider(BasicSpider):
    name = 'fundCompany'
    cal_client = CCalendar()
    logger = getLogger(__name__)
    allowed_domains = ['eastmoney.com']
    start_urls = ['http://fund.eastmoney.com/Company/default.html']
    # 表头仅存一次
    company_info_num = 0
    company_10stock_num = 0
    company_fundlist_num = 0
    industry_category_num = 0
    company_fundscale_num = 0
    company_info_url = "http://fund.eastmoney.com/Company/{}.html"
    company_10stock_url = "http://fund.eastmoney.com/Company/f10/gscc_{}.html"
    industry_category_url = "http://fund.eastmoney.com/Company/f10/hypz_{}.html"
    company_fundlist_url = "http://fund.eastmoney.com/Company/home/KFSFundNet?gsid={}&fundType={}"
    company_fundscale_url = "http://fund.eastmoney.com/Company/home/Gmtable?gsId={}&fundType={}"
    # 是否需要爬取以下内容
    need_company_info, need_company_fundscale, need_company_fundlist, need_company_10stock, need_industry_category = [1] * 5
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'SPIDERMON_ENABLED': True,
        'DOWNLOAD_DELAY': 1.0,
        'CONCURRENT_REQUESTS_PER_IP': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'SPIDERMON_VALIDATION_ADD_ERRORS_TO_ITEMS': True,
        'SPIDERMON_VALIDATION_ERRORS_FIELD': ct.SPIDERMON_VALIDATION_ERRORS_FIELD,
        'EXTENSIONS': {
            'spidermon.contrib.scrapy.extensions.Spidermon': 500,
        },
        'ITEM_PIPELINES': {
            'spidermon.contrib.scrapy.pipelines.ItemValidationPipeline': 200,
            'dspider.pipelines.DspiderPipeline': 300,
        },
        'SPIDERMON_UNWANTED_HTTP_CODES': ct.DEFAULT_ERROR_CODES,
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(FundCompanySpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_error(self, failure, response, spider):
        self.logger.error("wrong url:{}, failure:{}".format(response.url, failure.value))

    def spider_closed(self, spider, reason):
        result = self.company_info_num & self.company_10stock_num & self.company_fundlist_num & self.industry_category_num & self.company_fundscale_num
        succeed = "succeed" if result else "failed"
        message = 'scraped fund company info {}'.format(succeed)
        self.message = message
        self.collect_spider_info()

    def start_requests(self):
        self.company_info_num = 0
        self.company_10stock_num = 0
        self.company_fundlist_num = 0
        self.industry_category_num = 0
        self.company_fundscale_num = 0
        mdate = datetime.now().strftime('%Y-%m-%d')
        if self.cal_client.is_trading_day(mdate):
            url = self.start_urls[0]
            yield Request(url=url, callback=self.parse, meta={'date': mdate}, errback=self.errback_httpbin)

    # 1.主页基金列表
    def parse(self, response):
        try:
            if response.status != 200:
                self.logger.error('crawl page from url: {}, status: {} failed'.format(response.url, response.status))
                return
            mdate = response.meta['date']
            response = response.body.decode()
            response = etree.HTML(response)
            a_list = response.xpath("//div[@class='sencond-block']/a")
            for a in a_list:
                item = {"company_shortName": a.xpath("./text()")}
                if len(item["company_shortName"]) == 0:
                    item["company_shortName"] = None
                    continue
                item["company_shortName"] = item["company_shortName"][0]
                item["company_url"] = "http://fund.eastmoney.com" + a.xpath("./@href")[0]
                item["company_code"] = re.findall(r'Company/(.*?)\.html', item["company_url"])[0]
                # 2.1基金公司基本信息
                if self.need_company_info:
                    yield Request(self.company_info_url.format(item["company_code"]), callback=self.parse_company_info, meta={"item": item, "date": mdate}, errback=self.errback_httpbin)
                # 2.2基金公司股票型和混合型规模、数量、经理数量排名
                if self.need_company_fundscale:
                    for fundType in ["25", "27"]:  # funyType=25:股票型 27:混合型
                        yield Request(self.company_fundscale_url.format(item["company_code"], fundType), callback=self.parse_company_fundscale, meta={"item": item, "date": mdate}, errback=self.errback_httpbin)
                # 2.3基金公司下的基金清单
                if self.need_company_fundlist:
                    for funyType in ["001", "002"]:  # funyType=001是股票型 002是混合型
                        yield Request(self.company_fundlist_url.format(item["company_code"], funyType), callback=self.parse_company_fundList, meta={"item": item, "date": mdate}, errback=self.errback_httpbin)
                # 2.4公司的10大持仓股票
                if self.need_company_10stock:
                    yield Request(self.company_10stock_url.format(item["company_code"]), callback=self.parse_company_10stock, meta={"item": item, "date": mdate}, errback=self.errback_httpbin)
                # 2.5公司下的行业配置
                if self.need_industry_category:
                    yield Request(self.industry_category_url.format(item["company_code"]), callback=self.parse_company_industry_category, meta={"item": item, "date": mdate}, errback=self.errback_httpbin)
        except Exception as e:
            self.logger.error("get fund company info exception:{}".format(e))

    # 2.1基金公司基本信息
    def parse_company_info(self, response):
        item = response.meta.get("item")
        mdate = response.meta.get('date')
        company_code = item.get("company_code")
        company_shortName = item.get("company_shortName")
        response = etree.HTML(response.body.decode())
        item = {"company_code": company_code,
                "company_shortName": company_shortName,
                "company_name": response.xpath("//p[@class='ttjj-panel-main-title']/text()"),
                "position": response.xpath("//div[@class='firm-contact clearfix']/div[1]/p[1]/label/text()"),
                "general_manager": response.xpath("//div[@class='firm-contact clearfix']/div[1]/p[2]/label/text()"),
                "website_url": response.xpath("//div[@class='firm-contact clearfix']/div[2]/p[1]/label/text()"),
                "tell": response.xpath("//div[@class='firm-contact clearfix']/div[2]/p[2]/label/text()"),
                "manager_total_asset": response.xpath("//a[text()='管理规模']/../label/text()"),
                "fund_amount": response.xpath("//div[@class='fund-info']/ul/li[2]/label/a/text()"),
                "manager_amount": response.xpath("//div[@class='fund-info']/ul/li[3]/label/a/text()"),
                "publish_date": response.xpath("//div[@class='fund-info']/ul/li[5]/label/text()"),
                "company_property": response.xpath("//div[@class='fund-info']/ul/li[6]/label/text()")[0].strip()}
        for i_name in ['company_name', 'position', 'general_manager', 'website_url', 'tell', 'manager_total_asset', 'fund_amount', 'manager_amount', 'publish_date']:
            item[i_name] = item[i_name][0] if len(item[i_name]) > 0 else None
        self.save_data(item, mdate, ct.COMPANY_INFO_FILE_PATH, self.company_info_num)
        self.company_info_num = 1

    # 2.2基金公司股票型和混合型规模、数量、经理数量排名
    def parse_company_fundscale(self, response):
        item = response.meta.get("item")
        mdate = response.meta.get('date')
        company_code = item.get("company_code")
        company_shortName = item.get("company_shortName")
        response = etree.HTML(response.body.decode())
        item = {"company_code": company_code,
                "company_shortName": company_shortName,
                "fund_type": response.xpath("//tr[1]/th[2]/span/text()"),
                "fund_scale": response.xpath("//tr[2]/td[2]/text()"),
                "fund_scale_mean": response.xpath("//tr[2]/td[3]/text()"),
                "fund_scale_rank": response.xpath("//tr[2]/td[4]/text()"),
                "fund_amount": response.xpath("//tr[3]/td[2]/text()"),
                "fund_amount_mean": response.xpath("//tr[3]/td[3]/text()"),
                "fund_amount_rank": response.xpath("//tr[3]/td[4]/text()"),
                "fund_manager_amount": response.xpath("//tr[4]/td[2]/text()"),
                "fund_manager_amount_mean": response.xpath("//tr[4]/td[3]/text()"),
                "fund_manager_amount_rank": response.xpath("//tr[4]/td[4]/text()")}
        for i_name in ['fund_type', 'fund_scale', 'fund_scale_mean', 'fund_scale_rank', 'fund_amount',
                       'fund_amount_mean', 'fund_amount_rank', 'fund_manager_amount', 'fund_manager_amount_mean',
                       'fund_manager_amount_rank']:
            item[i_name] = item[i_name][0] if len(item[i_name]) > 0 else None
        self.save_data(item, mdate, ct.COMPANY_FUNDSCALE_PATH, self.company_fundscale_num)
        self.company_fundscale_num = 1

    # 2.3基金公司下的基金清单
    def parse_company_fundList(self, response):
        item = response.meta.get("item")
        mdate = response.meta.get('date')
        company_code = item.get("company_code")
        company_shortName = item.get("company_shortName")
        response = etree.HTML(response.body.decode())
        tr_list = response.xpath("//tbody/tr")
        for tr in tr_list:
            item = {"company_code": company_code,
                    "company_shortName": company_shortName,
                    "fund_name": tr.xpath("./td/a[1]/text()"),
                    "fund_code": tr.xpath("./td/a[2]/text()")}
            item["fund_name"] = item["fund_name"][0] if len(item["fund_name"]) > 0 else None
            item["fund_code"] = tr.xpath("./td/a[2]/text()")[0] if len(item["fund_code"]) > 0 else None
            self.save_data(item, mdate, ct.COMPANY_FUNDLIST_PATH, self.company_fundlist_num)
            self.company_fundlist_num = 1

    # 2.4公司的10大持仓股票
    def parse_company_10stock(self, response):
        mdate = response.meta.get('date')
        item = response.meta.get("item")
        company_code = item.get("company_code")
        company_shortName = item.get("company_shortName")
        response = etree.HTML(response.body.decode())
        tr_list = response.xpath("//table[@class='ttjj-table ttjj-table-hover']/tbody[1]/tr")
        for tr in tr_list:
            item = dict()
            item["company_code"] = company_code
            item["company_shortName"] = company_shortName
            item["stock_code"] = tr.xpath("./td[2]/a/text()")[0]
            item["stock_name"] = tr.xpath("./td[3]/a/text()")[0]
            item["havein_mycomanpy_fund"] = tr.xpath("./td[5]/a/text()")[0]  # 本公司持有基金数
            item["hold_in_value_percent"] = tr.xpath("./td[6]/text()")[0]  # 占总净值比例
            item["stock_amount"] = tr.xpath("./td[7]/text()")[0]  # 持股数(万股)
            item["stock_value"] = tr.xpath("./td[8]/text()")[0]  # 持仓市值(万元)
            self.save_data(item, mdate, ct.COMPANY_10STOCK_PATH, self.industry_category_num)
            self.industry_category_num = 1

    # 2.5公司下的行业配置
    def parse_company_industry_category(self, response):
        mdate = response.meta.get('date')
        item = response.meta.get("item")
        company_code = item.get("company_code")
        company_shortName = item.get("company_shortName")
        response = etree.HTML(response.body.decode())
        tr_list = response.xpath("//table[@class='ttjj-table ttjj-table-hover']//tr")[1:]  # [1:]去标题
        for tr in tr_list:
            industry_category_list = tr.xpath("./td[2]/text()")
            havein_mycomanpy_fund_list = tr.xpath("./td[4]/a/text()")
            hold_in_value_percent_list = tr.xpath("./td[5]/text()")
            stock_value_list = tr.xpath("./td[6]/text()")
            item = {"company_code": company_code,
                    "company_shortName": company_shortName,
                    "industry_category": industry_category_list[0] if len(industry_category_list) > 0 else None,
                    "havein_mycomanpy_fund": havein_mycomanpy_fund_list[0] if len(havein_mycomanpy_fund_list) > 0 else None,
                    "hold_in_value_percent": hold_in_value_percent_list[0] if len(hold_in_value_percent_list) > 0 else None,
                    "stock_value": stock_value_list[0] if len(stock_value_list) > 0 else None}
            self.save_data(item, mdate, ct.INDUSTRY_CATEGORY_PATH, self.company_fundscale_num)
            self.company_fundscale_num = 1

    def save_data(self, item, mdate, filedir, flag):
        file_path = "{}/{}.csv".format(filedir, mdate)
        with open(file_path, mode = "a+", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=item.keys())
            if flag == 0: writer.writeheader()
            writer.writerow(item)
