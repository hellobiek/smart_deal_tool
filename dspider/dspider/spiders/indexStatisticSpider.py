# -*- coding: utf-8 -*-
import scrapy
from dspider.items import IndexStatisticItem

def linesToDict(lines):
    """
    把逗号分隔的行，转换成字典
    """
    res={}
    for line in lines:
        if '=' in line:
            k,v=line.replace('var','').split('=')
            k=k.strip()
            v=v.replace('"','').replace('\r','')
            res[k]=v
    return res

kvmaping={
    #'pushDate':'zsgz00',
    #          静态市盈率      ,动态市盈率,    ,市净率        ,股息率         ,去年底静态市盈率  ,去年底滚动市盈率   ,去年底市净率
    '上证指数' :{'spe':'zsgz11','dpe':'zsgz12','pb':'zsgz13','dp':'zsgz18','lyspe':'zsgz14','lydpe':'zsgz15','lypb':'zsgz16'},
    '上证180' :{'spe':'zsgz21','dpe':'zsgz22','pb':'zsgz23','dp':'zsgz28','lyspe':'zsgz24','lydpe':'zsgz25','lypb':'zsgz26'},
    '上证50'  :{'spe':'zsgz31','dpe':'zsgz32','pb':'zsgz33','dp':'zsgz38','lyspe':'zsgz34','lydpe':'zsgz35','lypb':'zsgz36'},
    '沪深300' :{'spe':'zsgz41','dpe':'zsgz42','pb':'zsgz43','dp':'zsgz48','lyspe':'zsgz44','lydpe':'zsgz45','lypb':'zsgz46'},
    '深证成指' :{'spe':'zsgz51','dpe':'zsgz52','pb':'zsgz53','dp':'zsgz58','lyspe':'zsgz54','lydpe':'zsgz55','lypb':'zsgz56'},
    '深证100R':{'spe':'zsgz61','dpe':'zsgz62','pb':'zsgz63','dp':'zsgz68','lyspe':'zsgz64','lydpe':'zsgz65','lypb':'zsgz66'},
    '中小板指':{'spe':'zsgz71','dpe':'zsgz72','pb':'zsgz73','dp':'zsgz78','lyspe':'zsgz74','lydpe':'zsgz75','lypb':'zsgz76'},
    '上证380' :{'spe':'zsgz81','dpe':'zsgz82','pb':'zsgz83','dp':'zsgz88','lyspe':'zsgz84','lydpe':'zsgz85','lypb':'zsgz86'},
    '红利指数':{'spe':'zsgz91','dpe':'zsgz92','pb':'zsgz93','dp':'zsgz98','lyspe':'zsgz94','lydpe':'zsgz95','lypb':'zsgz96'},
    '中证红利':{'spe':'zsgz101','dpe':'zsgz102','pb':'zsgz103','dp':'zsgz108','lyspe':'zsgz104','lydpe':'zsgz105','lypb':'zsgz106'},
    '中证500' :{'spe':'zsgz111','dpe':'zsgz112','pb':'zsgz113','dp':'zsgz118','lyspe':'zsgz114','lydpe':'zsgz115','lypb':'zsgz116'}
}

def genItem(kvs,indexName):
    item             = IndexStatisticItem()
    item['push_date'] =kvs['zsgz00']
    item['index_name']=indexName
    variable=kvmaping[indexName]
    spe    =variable['spe']
    dpe    =variable['dpe']
    pb     =variable['pb']
    dp     =variable['dp']
    lyspe  =variable['lyspe']
    lydpe  =variable['lydpe']
    lypb   =variable['lypb']
    item['spe']=kvs[spe]
    item['dpe']=kvs[dpe]
    item['pb'] =kvs[pb]
    item['dp'] =kvs[dp]
    item['lyspe']=kvs[lyspe]
    item['lydpe']=kvs[lydpe]
    item['lypb']=kvs[lypb]
    return item

class IndexstatisticspiderSpider(scrapy.Spider):
    name = 'indexStatisticSpider'
    allowed_domains = ['www.csindex.com.cn']
    start_urls = ['http://www.csindex.com.cn/data/js/show_zsgz.js?str=z3l50GN6FTsOxMrb']

    def parse(self, response):
        dicts=linesToDict(response.text.split('\n'))
        for k in kvmaping:
            yield genItem(dicts,k)
