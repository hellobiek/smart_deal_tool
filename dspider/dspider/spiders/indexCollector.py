# -*- coding: utf-8 -*-
import scrapy
import json
import datetime
from items import IndexCollectorItem

#定义键值的映射关系
kvmaping={
    'index_name':'name',
    'open_value':'7',
    'higest_value':'8',
    'lowest_value':'9',
    'close_value':'10',
    'fluctuation':'526792',
    'transaction_amount':'19',
    'total_market_value':'92',
    'circulation_market_value':'90',
}


class IndexcollectorSpider(scrapy.Spider):
    name = 'IndexCollector'
    allowed_domains = ['d.10jqka.com.cn']
    start_urls = ['http://d.10jqka.com.cn/v2/realhead/zs_399006/last.js',#创业板指
                  'http://d.10jqka.com.cn/v2/realhead/zs_1B0016/last.js',#上证50
                  'http://d.10jqka.com.cn/v2/realhead/zs_1B0300/last.js',#沪深300
                  'http://d.10jqka.com.cn/v2/realhead/zs_1B0905/last.js',#中证500
                  'http://d.10jqka.com.cn/v2/realhead/zs_1A0001/last.js',#上证指数
                  'http://d.10jqka.com.cn/v2/realhead/zs_399001/last.js',#深证指数
                  'http://d.10jqka.com.cn/v2/realhead/zs_1B0015/last.js',#红利指数
                  ]


    def parse(self, response):
        """
        1、清洗数据，并把数据加载成json对象
        2、json对象中提取数据，并生成item
        """
        #清洗数据
        text=response.text
        index=text.index('(')
        text=text[index+1:-1]
        #生成json对象
        data=json.loads(text)
        item=data['items']
        #包装item
        index_item=IndexCollectorItem()
        for k in kvmaping:
            index_item[k]=item[kvmaping[k]]
        push_date=datetime.datetime.now()
        index_item['push_date']=push_date
        return index_item
