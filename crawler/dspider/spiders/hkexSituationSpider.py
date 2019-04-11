#coding=utf-8
import json
import scrapy
from datetime import datetime, timedelta
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError
from dspider.myspider import BasicSpider
from dspider.items import HkexTradeOverviewItem, HkexTradeTopTenItem
class HkexSpider(BasicSpider):
    name = 'hkexSpider'
    custom_settings = {
        'ITEM_PIPELINES': {
            'dspider.pipelines.DspiderPipeline': 2
        }
    }

    def start_requests(self):
        matching_url = "https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_{}c.js"
        end_date = datetime.now().strftime('%Y.%m.%d')
        start_date = self.get_nday_ago(end_date, 10, dformat = '%Y.%m.%d')
        while start_date <= end_date:  # 自己控制下时间范围
            start_date = self.get_tomorrow_date(sdate = start_date)
            url = matching_url.format(start_date.replace('.', ''))
            print(url)
            yield scrapy.Request(url=url, callback=self.parse, errback=self.errback_httpbin, dont_filter=True)

    def parse(self, response):
        try:
            jsonstr = response.text.split("=")[1]
            data = json.loads(jsonstr)
            sse_northbond = data[0]
            sse_northbond_overview_item = self.parseTradeOverviewItem(sse_northbond, "sse", "north")
            yield sse_northbond_overview_item
            sse_northbond_top_ten_items = self.parseTradeTopTenItem(sse_northbond, "sse", "north")
            for i in range(len(sse_northbond_top_ten_items)):
                yield sse_northbond_top_ten_items[i]

            sse_southbond = data[1]
            sse_southbond_overview_item = self.parseTradeOverviewItem(sse_southbond, "sse", "south")
            yield sse_southbond_overview_item
            sse_southbond_top_ten_items = self.parseTradeTopTenItem(sse_southbond, "sse", "south")
            for i in range(len(sse_southbond_top_ten_items)):
                yield sse_southbond_top_ten_items[i]

            szse_northbond = data[2]
            szse_northbond_overview_item = self.parseTradeOverviewItem(szse_northbond, "szse", "north")
            yield szse_northbond_overview_item
            szse_northbond_top_ten_items = self.parseTradeTopTenItem(szse_northbond, "szse", "north")
            for i in range(len(szse_northbond_top_ten_items)):
                yield szse_northbond_top_ten_items[i]

            szse_southbond = data[3]
            szse_southbond_overview_item = self.parseTradeOverviewItem(szse_southbond, "szse", "south")
            yield szse_southbond_overview_item
            szse_southbond_top_ten_items = self.parseTradeTopTenItem(szse_southbond, "szse", "south")
            for i in range(len(szse_southbond_top_ten_items)):
                yield szse_southbond_top_ten_items[i]
        except Exception as e:
            print(e)

    def parseTradeOverviewItem(self, need_parse_data, market, direction):
        trade_overview_tr = need_parse_data["content"][0]["table"]["tr"]
        item = HkexTradeOverviewItem()
        item['market'] = market
        item['direction'] = direction
        item['date'] = need_parse_data["date"]
        item['total_turnover'] = trade_overview_tr[0]["td"][0][0]
        item['buy_turnover'] = trade_overview_tr[1]["td"][0][0]
        item['sell_turnover'] = trade_overview_tr[2]["td"][0][0]
        item['total_trade_count'] = trade_overview_tr[3]["td"][0][0]
        item['buy_trade_count'] = trade_overview_tr[4]["td"][0][0]
        item['sell_trade_count'] = trade_overview_tr[5]["td"][0][0]
        if need_parse_data["market"] == "SSE Northbound" or need_parse_data["market"] == "SZSE Northbound":
            item['dqb'] = trade_overview_tr[6]["td"][0][0]
            item['dqb_ratio'] = trade_overview_tr[7]["td"][0][0]
        else:
            item['dqb'] = None
            item['dqb_ratio'] = None
        return item

    def parseTradeTopTenItem(self, need_parse_data, market, direction):
        items = []
        trade_top_ten_tr = need_parse_data["content"][1]["table"]["tr"]
        for i in range(10):
            item = HkexTradeTopTenItem()
            item['market'] = market
            item['direction'] = direction
            item['date'] = need_parse_data["date"]
            item['rank'] = trade_top_ten_tr[i]["td"][0][0]
            item['code'] = trade_top_ten_tr[i]["td"][0][1]
            item['name'] = trade_top_ten_tr[i]["td"][0][2].strip()
            item['buy_turnover'] = trade_top_ten_tr[i]["td"][0][3]
            item['sell_turnover'] = trade_top_ten_tr[i]["td"][0][4]
            item['total_turnover'] = trade_top_ten_tr[i]["td"][0][5]
            items.append(item)
        return items

    def errback_httpbin(self, failure):
        # log all errback failures, in case you want to do something special for some errors, you may need the failure's type
        #print(repr(failure))
        if failure.check(HttpError):
            response = failure.value.response
            #print('HttpError on %s', response.url)
        elif failure.check(DNSLookupError):
            request = failure.request
            #print('DNSLookupError on %s', request.url)
        elif failure.check(TimeoutError):
            request = failure.request
            #print('TimeoutError on %s', request.url)
