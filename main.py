# coding=utf-8
from gevent import monkey
monkey.patch_all()
import time
import traceback
import const as ct
from base.clog import getLogger 
from base.cthread import CThread
from data_manager import DataManager
from jobs.scheduler import Scheduler
log = getLogger(__name__)
def main():
    #time.sleep(200)
    threadList = []
    sc = Scheduler()
    dm = DataManager(ct.DB_INFO)
    log.info("init succeed")
    #threadList.append(CThread(dm.run, 600))
    threadList.append(CThread(dm.update, 300))
    threadList.append(sc.start())

    for thread in threadList:
        thread.start()

    for thread in threadList:
        thread.join()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(e)
        traceback.print_exc()
