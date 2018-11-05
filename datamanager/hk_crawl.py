#coding=utf-8
import time
import const as ct 
import pandas as pd
from log import getLogger
from bs4 import BeautifulSoup
from selenium import webdriver
from base.base import traditional2simplified
from selenium.webdriver.chrome.options import Options
class MSelenium:
    RETRY_TIME = 60
    def __init__(self, link, logger):
        self.link = link
        self.logger = logger
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-infobars')
        self.driver = webdriver.Chrome("/bin/chromedriver", chrome_options = options)

    def smart_call(self, func, except_result, *keys, **args):
        for i in range(MSelenium.RETRY_TIME):
            try:
                if 0 == len(keys):
                    if func() == except_result: return True
                else:
                    if func(keys[0]) == except_result: return True
            except:
                self.logger.debug("%s call with %s failed" % (func.__name__, keys))
            time.sleep(3)
        return False

    def smart_get(self, func, *keys, **args):
        for i in range(MSelenium.RETRY_TIME):
            try:
                if 0 == len(keys):
                    return func()
                else:
                    return func(keys[0])
            except:
                self.logger.debug("wait for find element")
            time.sleep(3)
        return None

    def process(self, req_date):
        [year, month, day] = req_date.split('-')
        if not self.smart_call(self.driver.get, None, self.link):
            self.logger.error("get source page failed.")
            return None

        if not self.smart_call(self.driver.set_page_load_timeout, None, 15):
            self.logger.error("set page load timeout failde.")
            return None

        element = self.smart_get(self.driver.find_element_by_id, 'txtShareholdingDate')
        if element is None:
            self.logger.error("find txtShareholdingDate element by xpath failed.")
            return None

        if not self.smart_call(element.click, None):
            self.logger.error("get txtShareholdingDate element by xpath failed.")
            return None

        element = self.smart_get(self.driver.find_element_by_xpath, "//b[@class='year']//button[@data-value=%s]" % year)
        if element is None:
            self.logger.error("find year element by xpath failed.")
            return None

        if not self.smart_call(element.click, None):
            self.logger.error("get find year element by xpath failed.")
            return None

        element = self.smart_get(self.driver.find_element_by_xpath, "//b[@class='month']//button[@data-value=%s]" % (int(month) - 1))
        if element is None:
            self.logger.error("find month element by xpath failed.")
            return None

        if not self.smart_call(element.click, None):
            self.logger.error("get month element by xpath failed.")
            return None

        element = self.smart_get(self.driver.find_element_by_xpath, "//b[@class='day']//button[@data-value=%s]" % int(day))
        if element is None:
            self.logger.error("find day element by xpath failed.")
            return None

        if not self.smart_call(element.click, None):
            self.logger.error("get day element by xpath failed.")
            return None

        element = self.smart_get(self.driver.find_element_by_name, "btnSearch")
        if element is None:
            self.logger.error("find search button by xpath failed.")
            return None

        if not self.smart_call(element.click, None):
            self.logger.error("get search button result by xpath failed.")
            return None
        return self.driver.page_source

class MCrawl:
    LINK_PATH = 'http://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=%s'
    def __init__(self, type_):
        self.link       = self.LINK_PATH % type_
        self.type       = type_
        self.logger     = getLogger(__name__)
        self.crawer     = MSelenium(self.link, self.logger)

    def transfer_code(self, code):
        if code.startswith("9"):
            return code.replace("9", "60", 1)
        elif code.startswith("70"):
            return code.replace("70", "000", 1)
        elif code.startswith("72"):
            return code.replace("70", "002", 1)
        elif code.startswith("77"):
            return code.replace("77", "300", 1)
        else:
            return code

    def crawl(self, req_date = None):
        result = self.crawer.process(req_date)
        if result is None:
            self.driver.refresh()
            return 1, pd.DataFrame()
        soup      = BeautifulSoup(result, "html.parser")
        real_date = soup.select("h2.ccass-heading")[0].span.text.strip().split(":")[1].strip()
        if req_date != real_date.replace('/', '-'): return 1, pd.DataFrame()
        data = []
        rows = soup.select("table#mutualmarket-result")[0].find_all('tr')
        for index in range(1, len(rows)):
            items = rows[index].get_text().strip().split('\n\n')
            code_str = items[0].strip().split(':')[1].strip()
            code     = code_str if self.type == ct.HK_MARKET_SYMBOL else self.transfer_code(code_str)
            name     = traditional2simplified(items[1].strip().split(':')[1].strip())
            quanity  = int(items[2].strip().split(':')[1].strip().replace(',', ''))
            percent  = float(items[3].strip().split(':')[1].strip('%'))
            data.append({"code": code, "name": name, "volume": quanity, "percent": percent})
        return 0, pd.DataFrame(data, columns=["code", "name", "volume", "percent"])
