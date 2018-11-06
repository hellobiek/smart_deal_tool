# -*- coding: utf-8 -*-
import scrapy
import utils, items
investor_count_to_path={
    "new_investor"            :"//*[@id='settlementList']/table/tbody/tr/td/table/tbody/tr[2]/td[2]/p/span/text()", #新增投资者数量
    "final_investor"          :"//*[@id='settlementList']/table/tbody/tr/td/table/tbody/tr[5]/td[2]/p/span/text()", #期末投资者数量
    "new_natural_person"      :"//*[@id='settlementList']/table/tbody/tr/td/table/tbody/tr[3]/td[2]/p/span/text()", #新增投资者中自然人数量
    "new_non_natural_person"  :"//*[@id='settlementList']/table/tbody/tr/td/table/tbody/tr[4]/td[2]/p/span/text()", #新境投资者中非自然人数量
    "final_natural_person"    :"//*[@id='settlementList']/table/tbody/tr/td/table/tbody/tr[6]/td[2]/p/span/text()", #期末投资者中自然人数量
    "final_non_natural_person":"//*[@id='settlementList']/table/tbody/tr/td/table/tbody/tr[10]/td[2]/p/span/text()",#期末投资都中非自然人数量
    "unit"                    :"//*[@id='settlementList']/table/tbody/tr/td/table/tbody/tr[1]/td[2]/p/strong/span/text()"
}

class InvestorsituationspiderSpider(scrapy.Spider):
    name = 'investorSituationSpider'
    allowed_domains = ['www.chinaclear.cn']
    start_urls = ['http://www.chinaclear.cn/cms-search/view.action']
    def parse(self, response):
        investor_situation_item = items.InvestorSituationItem()
        for k in investor_count_to_path:
            investor_situation_item[k]=response.xpath(investor_count_to_path[k]).extract_first().strip()
        investor_situation_item['push_date']=utils.datetime_to_str()
        return investor_situation_item
