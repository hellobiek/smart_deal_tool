import sys
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen

def parser(address):
    html = urlopen(address).read()
    soup = BeautifulSoup(html, "lxml")
    table_html = soup.find(id="REPORTID_tab1")
    df = pd.read_html(table_html.prettify())[0]
    df.columns = df.loc[0].tolist()
    df = df.drop([0], axis = 0)

if __name__ == '__main__':
    address = "https://www.szse.cn/main/disclosure/news/tfpts/" 
    parser(address)
    
