# -*- coding: utf-8 -*-
import re
import json
import const as ct
import numpy as np
import pandas as pd
from pathlib import Path
from common import add_suffix
from scrapy import FormRequest, Selector
from datetime import datetime
from dspider.myspider import BasicSpider
class HeroListSpider(BasicSpider):
    name = 'herolistspider'
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
        'SPIDERMON_VALIDATION_MODELS': {
            ###StockLimitItem: 'dspider.validators.StockLimitModel',
        },
        'SPIDERMON_SPIDER_CLOSE_MONITORS': (
            'dspider.monitors.SpiderCloseMonitorSuite',
        )
    }
    def start_requests(self):
        matching_url = "http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=300,page=1,sortRule=-1,sortType=,startDate={},endDate={},gpfw=0,js=var%20data_tab_1.html?rt=26442172"
        start_date = datetime.now().strftime('%Y-%m-%d')
        url = matching_url.format(start_date, start_date)
        yield FormRequest(url=url, callback=self.parse_meta, errback=self.errback_httpbin)

    def parse_meta(self, response):
        try:
            jsonstr = response.text.split("data_tab_1=")[1].strip()
            info = json.loads(jsonstr)
            data = info['data']
            df = pd.DataFrame(data)
            if df.empty: return
            df = df[['Tdate', 'SCode', 'SName','JD','ClosePrice', 'Chgradio',\
                     'JmMoney', 'Bmoney', 'Smoney', 'ZeMoney', 'Turnover',\
                     'JmRate', 'ZeRate', 'Dchratio', 'Ltsz', 'Ctypedes']]
            colunms_name = ['code', 'name', '解读', '收盘价', 'pchange',\
                            '净买额', '买入额', '卖出额', '成交额',\
                            '市场总成交额', '净买额占总成交比', '成交额占比',\
                            '换手率', '流通市值', '上榜原因']
            df = df.rename(columns = {'Tdate': 'date', 'SCode': colunms_name[0],\
                                      'SName':colunms_name[1], 'JD': colunms_name[2],\
                                      'ClosePrice': colunms_name[3], 'Chgradio': colunms_name[4],\
                                      'JmMoney': colunms_name[5], 'Bmoney': colunms_name[6],\
                                      'Smoney':colunms_name[7], 'ZeMoney':colunms_name[8],\
                                      'Turnover':colunms_name[9], 'JmRate':colunms_name[10],\
                                      'ZeRate':colunms_name[11], 'Dchratio':colunms_name[12],\
                                      'Ltsz':colunms_name[13], 'Ctypedes':colunms_name[14]})
            df['code'] = df['code'].map(lambda x : str(x).zfill(6))
            df = df.loc[df['code'].str.startswith('0') | df['code'].str.startswith('6') | df['code'].str.startswith('3')]
            df = df.reset_index(drop = True)
            for id_, row in df.iterrows():
                page_url = 'http://data.eastmoney.com/stock/lhb,{},{}.html'.format(row['date'], row['code'])
                yield FormRequest(url = page_url, meta={'pchange': row['pchange'], 'name': row['name']}, method = 'GET', callback = self.parse_item, errback=self.errback_httpbin)
        except Exception as e:
            print(e)

    def html_parser(self, link_tables):
        table_list = []
        for ind, link_table in enumerate(link_tables):
            links = link_table.xpath('.//tr')        
            for ind2, link2 in enumerate(links):
                sc_name = link2.xpath('.//td//div[@class="sc-name"]//a//text()').extract()
                if len(sc_name) > 0:
                    if sc_name[0].find('机构专用') >= 0:
                        net_buys = link2.xpath('.//td[@style="color:red"]//text()').extract()
                        net_sells = link2.xpath('.//td[@style="color:Green"]//text()').extract()
                        net_buy = float(net_buys[0]) if len(net_buys) > 0 else 0
                        net_sell = float(net_sells[0]) if len(net_sells) > 0 else 0
                        table_list.append([sc_name[0], net_buy, net_sell])
        table_data = pd.DataFrame()
        if len(table_list) > 0:
            table_data = pd.DataFrame(table_list)
            table_data = table_data.rename(columns = {0:'sec_name', 1:'buy', 2:'sell'})
            table_data['net'] = table_data['buy'] - table_data['sell']
        return table_data

    def store_items(self, mdate, data):
        filepath = Path(ct.STOCK_TOP_LIST_DATE_PATH)/"{}.csv".format(mdate)
        if not filepath.exists():
            data.to_csv(filepath, index=False, header=True, mode='w', encoding='utf8')
        else:
            data.to_csv(filepath, index=False, header=False, mode='a+', encoding='utf8')

    def parse_item(self, response):
        try:
            name = response.meta['name']
            pchange = round(float(response.meta['pchange']), 2)
            content = response.text
            selector = Selector(text = content).xpath('//div[@class="data-tips"]//div[@class="left con-br"]//text()').extract()
            for index in range(len(selector)):
                stype = selector[index].split('类型：')[1]
                stype = "3日" if stype.find("三个交易日") >= 0 or stype.find("3个交易日") >= 0 else "1日"
                buy_links = Selector(text = content).xpath('//div[@class="content-sepe"]//table[@class="default_tab stock-detail-tab"]//tbody')
                sell_links = Selector(text = content).xpath('//div[@class="content-sepe"]//table[@class="default_tab tab-2"]//tbody')
                top_buy_data = self.html_parser([buy_links[index]])
                top_sell_data = self.html_parser([sell_links[index]])
                net_buy_value = int(top_buy_data['net'].sum()) if not top_buy_data.empty else 0
                net_sell_value = int(abs(top_sell_data['net'].sum())) if not top_sell_data.empty else 0
                _, mdate, code = response.url.split(',')
                code = code.split('.')[0]
                if net_buy_value > 0 or net_sell_value > 0:
                    info = pd.DataFrame([[mdate, code, name, stype, pchange, net_buy_value, net_sell_value]], columns=['date', 'code', 'name', 'type', 'pchange', 'buy', 'sell'])
                    self.store_items(mdate, info)
        except Exception as e:
            print(e)
