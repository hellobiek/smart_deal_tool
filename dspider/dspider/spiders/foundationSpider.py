# -*- coding: utf-8 -*-
import scrapy
from items import FoundationBriefItem
class FoundationspiderSpider(scrapy.Spider):
    name = 'foundationSpider'
    allowed_domains = ['www.cninfo.com.cn']
    start_urls = ['http://www.cninfo.com.cn/cninfo-new/index']

    def parse_foundation_brief(self,response):
        #基金代码、基金名称所在的xpath
        foundation_code_and_name_xpath="//*[@id='cninfoform']/table/tr/td[1]/text()"
        found_code=response.xpath(foundation_code_and_name_xpath)[0].extract().strip()
        found_name=response.xpath(foundation_code_and_name_xpath)[1].extract().strip()
        #基金管理公司(基础管理人)
        foundation_manager_company_xpath="/html/body/div[2]/div[1]/div[2]/table/tr[1]/td[2]/text()"
        found_manager_company=response.xpath(foundation_manager_company_xpath).extract_first().strip()
        #基金类型
        foundation_type_xpath="/html/body/div[2]/div[1]/div[2]/table/tr[5]/td[2]/text()"
        found_type=response.xpath(foundation_type_xpath).extract_first().strip()
        #基金成立日期
        foundation_birth_xpath="/html/body/div[2]/div[1]/div[2]/table/tr[6]/td[2]/text()"
        found_birth=response.xpath(foundation_birth_xpath).extract_first().strip()
        #交易所
        foundation_exchange_xpath="/html/body/div[2]/div[1]/div[2]/table/tr[8]/td[2]/text()"
        found_exchange=response.xpath(foundation_exchange_xpath).extract_first().strip()
        #创建Item
        found_item                         =FoundationBriefItem()
        found_item['found_code']           =found_code
        found_item['found_name']           =found_name
        found_item['found_manager_user']   =''
        found_item['found_manager_company']=found_manager_company
        found_item['found_type']           =found_type
        found_item['found_birth']          =found_birth
        found_item['found_exchange']       =found_exchange
        yield found_item

    def parse_foundation_list(self,response):
        """
        得到单个基金所在页面的url,并返回一个与这个url对应的Request对象
        """
        #深交所上市基金信息
        shen_market_foundation_list=response.xpath("//*[@id='con-a-1']/ul/li")
        foundation_xpath="//*[@id='con-a-1']/ul/li[{0}]/a/@href"
        doundation_brief_url="http://www.cninfo.com.cn/information/fund/brief/{0}.html"
        for index in range(1,len(shen_market_foundation_list)+1):
            foundation_page_url=response.xpath(foundation_xpath.format(index)).extract_first()
            foundation_code    =foundation_page_url.split('?')[2]
            yield scrapy.http.Request(url=doundation_brief_url.format(foundation_code),callback=self.parse_foundation_brief)

        #上交所上市基金信息
        shang_market_foundation_list=response.xpath("//*[@id='con-a-2']/ul/li")
        foundation_xpath="//*[@id='con-a-2']/ul/li[{0}]/a/@href"
        doundation_brief_url="http://www.cninfo.com.cn/information/fund/brief/{0}.html"
        for index in range(1,len(shang_market_foundation_list)+1):
            foundation_page_url=response.xpath(foundation_xpath.format(index)).extract_first()
            foundation_code    =foundation_page_url.split('?')[2]
            yield scrapy.http.Request(url=doundation_brief_url.format(foundation_code),callback=self.parse_foundation_brief)

        #其它
        other_market_foundation_list=response.xpath("//*[@id='con-a-3']/ul/li")
        foundation_xpath="//*[@id='con-a-3']/ul/li[{0}]/a/@href"
        doundation_brief_url="http://www.cninfo.com.cn/information/fund/brief/{0}.html"
        for index in range(1,len(shang_market_foundation_list)+1):
            foundation_page_url=response.xpath(foundation_xpath.format(index)).extract_first()
            foundation_code    =foundation_page_url.split('?')[2]
            yield scrapy.http.Request(url=doundation_brief_url.format(foundation_code),callback=self.parse_foundation_brief)

    def parse(self, response):
        """
        得到一个基金列表页面的url，并返回一个与这个url对应的Request对象
        """
        foundation_list_url=response.xpath("/html/body/div[1]/div/div[4]/li[2]/div/ul[1]/dd[2]/a/@href").extract_first()
        foundation_list_url=response.urljoin(foundation_list_url)
        yield scrapy.http.Request(url=foundation_list_url,callback=self.parse_foundation_list)
