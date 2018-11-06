# -*- coding: utf-8 -*-
import scrapy
from items import ShiborItem

#define rate name to xpath 
rate_type_to_path={
    "one_night"  :"/descendant::table[position()=4]/tr[position()=1]/td[position()=3]/text()",
    "one_week"   :"/descendant::table[position()=4]/tr[position()=2]/td[position()=3]/text()",
    "two_week"   :"/descendant::table[position()=4]/tr[position()=3]/td[position()=3]/text()",
    "one_month"  :"/descendant::table[position()=4]/tr[position()=4]/td[position()=3]/text()",
    "three_month":"/descendant::table[position()=4]/tr[position()=5]/td[position()=3]/text()",
    "six_month"  :"/descendant::table[position()=4]/tr[position()=6]/td[position()=3]/text()",
    "nine_month" :"/descendant::table[position()=4]/tr[position()=7]/td[position()=3]/text()",
    "one_year"   :"descendant::table[position()=4]/tr[position()=8]/td[position()=3]/text()",
    "push_date"  :"/descendant::table[position()=2]/tr[position()=1]/td/text()"
}

class ShiborspiderSpider(scrapy.Spider):
    """
    ShiborspiderSpider 用于完成对shibor利率的数据收集，返回ShiborItem类的对象
    """
    name = 'shiborSpider'
    allowed_domains = ['www.shibor.org']
    start_urls = ['http://www.shibor.org/shibor/web/html/shibor.html']

    def parse(self, response):
        """
        parse response and yield ShiborItem object
        """
        shibor_item=ShiborItem()
        for rate_name  in rate_type_to_path:
            path=rate_type_to_path[rate_name]
            shibor_item[rate_name]=response.xpath(path).extract_first().strip()
        return shibor_item
