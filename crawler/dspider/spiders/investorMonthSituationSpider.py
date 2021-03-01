# -*- coding: utf-8 -*-
import re
import datetime
import const as ct
from scrapy import signals
from datetime import datetime
from scrapy import FormRequest
from base.clog import getLogger
from dspider.utils import datetime_to_str
from dspider.myspider import BasicSpider
from dspider.items import MonthInvestorSituationItem
investor_count_to_path = {
    "date"                       : "/html[1]/body[1]/form[1]/div[2]/h2[1]/text()",#月份
    "unit"                       : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[1]/td[2]/p[1]/span[1]/text()",  #单位
    "new_investor"               : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[2]/td[2]/p[1]/span[1]/text()",  #新增投资者数量
    "new_natural_person"         : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[3]/td[2]/p[1]/span[1]/text()",  #新增投资者中自然人数量
    "new_non_natural_person"     : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[4]/td[2]/p[1]/span[1]/text()",  #新境投资者中非自然人数量
    "final_investor"             : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[5]/td[2]/p[1]/span[1]/text()",  #期末投资者数量
    "final_natural_person"       : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[6]/td[2]/p[1]/span[1]/text()",  #期末投资者中自然人数量
    "final_natural_a_person"     : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[8]/td[2]/p[1]/span[1]/text()",  #期末A股投资者中自然人数量
    "final_natural_b_person"     : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[9]/td[2]/p[1]/span[1]/text()",  #期末B股投资者中自然人数量
    "final_non_natural_person"   : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[10]/td[2]/p[1]/span[1]/text()", #期末投资者中非自然人数量
    "final_non_natural_a_person" : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[12]/td[2]/p[1]/span[1]/text()", #期末A股投资者中非自然人数量
    "final_non_natural_b_person" : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[13]/td[2]/p[1]/span[1]/text()", #期末B股投资者中非自然人数量
    "final_hold_investor"        : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[14]/td[2]/p[1]/span[1]/text()", #期末持仓投资者数量
    "final_a_hold_investor"      : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[16]/td[2]/p[1]/span[1]/text()", #期末A股持仓投资者数量
    "final_b_hold_investor"      : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[17]/td[2]/p[1]/span[1]/text()", #期末B股持仓投资者数量
    "trading_investor"           : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[18]/td[2]/p[1]/span[1]/text()", #期间参与交易的投资者数量
    "trading_a_investor"         : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[20]/td[2]/p[1]/span[1]/text()", #期间参与A股交易的投资者数量
    "trading_b_investor"         : "/html[1]/body[1]/form[1]/div[3]/div[1]//*/table[1]/tbody[1]/tr[21]/td[2]/p[1]/span[1]/text()"  #期间参与B股交易的投资者数量
}

class MonthInvestorSituationSpider(BasicSpider):
    name = 'monthInvestorSituationSpider'
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
            'spidermon.contrib.scrapy.pipelines.ItemValidationPipeline': 100,
            'dspider.pipelines.DspiderPipeline': 200,
        },
        'SPIDERMON_UNWANTED_HTTP_CODES': ct.DEFAULT_ERROR_CODES,
        'SPIDERMON_VALIDATION_MODELS': {
            MonthInvestorSituationItem: 'dspider.validators.MonthInvestorSituationModel',
        },
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }
    logger = getLogger(__name__)
    allowed_domains = ['www.chinaclear.cn']
    start_url = 'http://www.chinaclear.cn/cms-search/monthview.action?action=china'
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MonthInvestorSituationSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(spider.item_dropped, signal=signals.item_dropped)
        crawler.signals.connect(spider.item_error, signal=signals.item_error)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def item_error(self, item, response, spider, failure):
        self.logger.error("wrong item date:{}, failure:{}".format(item['date'], failure.value))

    def item_scraped(self, item, response, spider):
        pass

    def spider_error(self, failure, response, spider):
        self.logger.error("wrong url:{}, failure:{}".format(response.url, failure.value))

    def item_dropped(self, item, spider, exception):
        self.logger.error("exception date:{}, failure:{}".format(item['date'], exception))

    def spider_closed(self, spider, reason):
        message = 'run at {}'.format(datetime.now().strftime('%Y年%m月'))
        self.message = message
        self.status = True
        self.collect_spider_info()

    def start_requests(self):
        formdata = dict()
        formdata['channelIdStr'] = '08ce523457dd47d2aad6b41246964535'
        formdata['channelFidStr'] = '4f8a220e5ca04a388ca4bae0d1226d0d'
        end_month = datetime.now().strftime('%Y年%m月')
        start_month = self.get_nmonth_ago(end_month, 3)
        while start_month < end_month:
            formdata['riqi'] = start_month
            yield FormRequest(url = self.start_url, method = 'POST', formdata = formdata, callback = self.parse, errback=self.errback_httpbin)
            start_month = self.get_next_month(smonth = start_month)

    def parse(self, response):
        patten = re.compile(r'[(|（](.*?)[）|)]', re.S)
        item = MonthInvestorSituationItem()
        tmpStr = response.xpath("/html[1]/body[1]/form[1]/div[3]/div[1]/font[1]/text()").extract_first()
        if (tmpStr is not None and tmpStr.find('没有找到相关信息，请检查查询条件') != -1): return
        unit = '万'
        for k in investor_count_to_path:
            if k == "date":
                tmpstr = response.xpath(investor_count_to_path[k]).extract_first().strip()
                if tmpstr == '搜索结果': return
                mdate = re.findall(patten, tmpstr)[0].strip()
                mdate = mdate.replace('年', '-')
                mdate = mdate.replace('月', '')
                item[k] = mdate
            elif k == 'unit':
                item[k] = unit
            else:
                value_str = response.xpath(investor_count_to_path[k]).extract_first()
                value_str = value_str.strip() if value_str is not None else None
                item[k] = item.convert_unit(value_str, unit, float)
        yield item
