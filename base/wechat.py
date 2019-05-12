# coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
from base.clog import getLogger
from base.net.client import Client
from spidermon.core.actions import Action
logger = getLogger(__name__)
class SendWechat(Action):
    def __init__(self, *args, **kwargs):
        super(SendWechat, self).__init__(*args, **kwargs)
        self.client = Client()
        self.key = self.load_key(ct.WECHAT_FILE)

    @classmethod
    def from_crawler_kwargs(cls, crawler):
        kwargs = super(SendWechat, cls).from_crawler_kwargs(crawler)
        return kwargs

    def load_key(self, fpath):
        with open(fpath) as f: key = f.read().strip()
        return key

    def run_action(self):
        for item in self.result.monitors_failed_results:
            content = item.reason
            title = item.monitor.name
            self.send_message(title, content)

    def send_message(self, title, content):
        url = 'https://sc.ftqq.com/%s.send' % self.key
        data = {}
        data['text'] = title
        data['desp'] = content
        ret, result = self.client.post(url, data)
        if ret != 0: logger.error("ret:%s, result:%s" % (ret, result))
