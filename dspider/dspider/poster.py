import requests
import datetime
class Poster(object):
    server_url="http://www.financedatas.com/component/"
    api=""
    def __init__(self,item):
        self._item=item

    @property
    def http_api(self):
        return self.server_url+self.api

    def post(self):
        response=requests.post(self.http_api,data=self._item.convert())
        print('-'*64)
        print(self._item.convert())
        print(response.text)
        print('-'*64)

class ShiborItemPoster(Poster):
    #server_url="http://127.0.0.1:8000/component/"
    api="market/add/shiborrate/"

class InvestorSituationItemPoster(Poster):
    #server_url="http://127.0.0.1:8000/component/"
    api="market/add/investorsituation/"

class IndexCollectorItemPoster(Poster):
    api="market/add/update/stockindex/"
    def post(self):
        data=self._item.convert()
        if datetime.datetime.now().hour >=17:
            response=requests.post(self.http_api,data=data)
            print('-'*64)
            print(response)
            print('-'*64)
            pass
            #上传数据到服务器
        else:
            #还没有收盘，数据不上传
            print('-'*64)
            print(data)
            print('-'*64)
            
class IndexStatisticItemPoster(Poster):
    api="market/add/update/stockindex/"

class FoundationBriefItemPoster(Poster):
    #server_url="http://127.0.0.1:8000/component/"
    api="market/add/update/foundationbrief/"
