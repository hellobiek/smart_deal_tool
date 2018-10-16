# coding=utf-8
import gevent
from gevent import monkey
monkey.patch_all(subprocess=True)
import json
import gear
import pandas as pd
import _pickle
import traceback
import const as ct
from common import create_redis_obj
from log import getLogger
log = getLogger(__name__)
def worker(client_id, func_name, df, key, subset):
    worker = gear.Worker(client_id)
    worker.addServer(host=ct.GEARMAND_HOST, port=ct.GEARMAND_PORT)
    worker.registerFunction(func_name)
    redis = create_redis_obj()
    while True:
        job = worker.getJob()
        info = json.loads(job.arguments.decode('utf-8'))
        tmp_df = pd.DataFrame(info, index=[0])
        tmp_redis = redis.get(key)
        if tmp_redis is not None: df = _pickle.loads(tmp_redis)
        df = df.append(tmp_df)
        df = df.drop_duplicates(subset)
        redis.set(key, _pickle.dumps(df, 2))
        job.sendWorkComplete()

def main():
    objlist = []
    objlist.append(gevent.spawn_later(1, worker, "1", ct.SYNCSTOCK2REDIS, pd.DataFrame(), ct.STOCK_INFO, ['code']))
    objlist.append(gevent.spawn_later(1, worker, "2", ct.SYNCCAL2REDIS, pd.DataFrame(), ct.CALENDAR_INFO, ['calendarDate']))
    objlist.append(gevent.spawn_later(1, worker, "3", ct.SYNC_COMBINATION_2_REDIS, pd.DataFrame(), ct.COMBINATION_INFO, ['code']))
    objlist.append(gevent.spawn_later(1, worker, "4", ct.SYNC_HALTED_2_REDIS, pd.DataFrame(), ct.HALTED_INFO, ['code','date']))
    objlist.append(gevent.spawn_later(1, worker, "5", ct.SYNC_DELISTED_2_REDIS, pd.DataFrame(), ct.DELISTED_INFO, ['code']))
    objlist.append(gevent.spawn_later(1, worker, "6", ct.SYNC_ANIMATION_2_REDIS, pd.DataFrame(), ct.ANIMATION_INFO, ['date','time','name']))
    gevent.joinall(objlist)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(e)
        traceback.print_exc()
