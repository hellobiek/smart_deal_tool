#coding:utf-8
import random
'''
这个类主要用于产生随机UserAgent
'''
class RandomUserAgent(object):
    def __init__(self, agents, headers):
        self.agents = agents
        self.headers = headers

    @classmethod
    def from_crawler(cls, crawler):
        #返回的是本类的实例cls == RandomUserAgent
        return cls(
            agents = crawler.settings.getlist('USER_AGENTS'),
            headers = crawler.settings.getdict('DEFAULT_REQUEST_HEADERS')
        )

    def process_request(self, request, spider):
        request.headers = self.headers 
        request.headers.setdefault('User-Agent', random.choice(self.agents))
