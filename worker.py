# coding=utf-8
import gevent
from gevent import monkey
monkey.patch_all(subprocess=True)
import gevent.pywsgi
import sys
import json
import time
import gear
import pandas
import _pickle
import traceback
import const as ct
from common import create_redis_obj
from log import getLogger

log = getLogger(__name__)

def worker(client_id, func_name, df, key, subset):
    # create worker and listen for specific queue pipe
    worker = gear.Worker(client_id)
    worker.addServer(host=ct.GEARMAND_HOST, port=ct.GEARMAND_PORT)
    worker.registerFunction(func_name)
    redis = create_redis_obj()
    while True:
        job = worker.getJob() 
        info = json.loads(job.arguments.decode('utf-8'))
        tmp_df = pandas.DataFrame(info, index=[0])
        df = _pickle.loads(redis.get(key))
        df = df.append(tmp_df)
        df = df.drop_duplicates(subset)
        redis.set(key, _pickle.dumps(df, 2))
        job.sendWorkComplete()

def main():
    objlist = []
    objlist.append(gevent.spawn_later(1, worker, "1", ct.SYNCSTOCK2REDIS, pandas.DataFrame(), ct.STOCK_INFO, ['code']))
    objlist.append(gevent.spawn_later(1, worker, "2", ct.SYNCCAL2REDIS, pandas.DataFrame(), ct.CALENDAR_INFO, ['calendarDate']))
    objlist.append(gevent.spawn_later(1, worker, "3", ct.SYNC_COMBINATION_2_REDIS, pandas.DataFrame(), ct.COMBINATION_INFO, ['code']))
    objlist.append(gevent.spawn_later(1, worker, "4", ct.SYNC_HALTED_2_REDIS, pandas.DataFrame(), ct.HALTED_INFO, ['code','date']))
    objlist.append(gevent.spawn_later(1, worker, "5", ct.SYNC_DELISTED_2_REDIS, pandas.DataFrame(), ct.DELISTED_INFO, ['code']))
    gevent.joinall(objlist)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(e)
        traceback.print_exc()
