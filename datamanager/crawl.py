import pandas as pd
from bs4 import BeautifulSoup
from langconv import Converter
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
def traditional2simplified(sentence):
    return Converter('zh-hans').convert(sentence)

class SrcSelenium:
    def __init__(self, req_date, link):
        self.link = link
        self.req_date = req_date
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-infobars')
        self.driver = webdriver.Chrome("/Users/hellobiek/Documents/workspace/python/quant/hkexnews/crawls/chromedriver", chrome_options = options)

    def __del__(self):
        self.driver.close()
        self.driver.quit()

    def process(self):
        self.driver.get(self.link)
        self.driver.set_page_load_timeout(15)
        try:
            self.driver.find_element_by_id('txtShareholdingDate').click()
        except:
            pass
        [year, month, day] = self.req_date.split('-')
        self.driver.find_element_by_xpath("//b[@class='year']//button[@data-value=%s]" % year).click()
        self.driver.find_element_by_xpath("//b[@class='month']//button[@data-value=%s]" % (int(month) - 1)).click()
        self.driver.find_element_by_xpath("//b[@class='day']//button[@data-value=%s]" % int(day)).click()
        btnSearch   = self.driver.find_element_by_name("btnSearch")
        btnSearch.click()
        return self.driver.page_source

class MCrawl:
    RET_ERR = -1
    LINK_PATH = 'http://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=%s'
    def __init__(self, req_date, type_):
        self.link       = self.LINK_PATH % type_
        self.type       = type_
        self.req_date   = req_date

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

    def crawl(self):
        soup      = BeautifulSoup(SrcSelenium(self.req_date, self.link).process(), "html.parser")
        real_date = soup.select("h2.ccass-heading")[0].span.text.strip().split(":")[1].strip()
        if self.req_date != real_date.replace('/', '-'): return RET_ERR, pd.DataFrame()
        data = []
        rows = soup.select("table#mutualmarket-result")[0].find_all('tr')
        for index in range(1, len(rows)):
            items = rows[index].get_text().strip().split('\n\n')
            code_str = items[0].strip().split(':')[1].strip()
            code     = code_str if self.type == "HK" else self.transfer_code(code_str)
            name     = traditional2simplified(items[1].strip().split(':')[1].strip())
            quanity  = items[2].strip().split(':')[1].strip()
            precent  = items[3].strip().split(':')[1].strip()
            data.append({"code": code, "name": name, "volume": quanity, "percent": precent})
        return 0, pd.DataFrame(data, columns=["code", "name", "volume", "percent"])
