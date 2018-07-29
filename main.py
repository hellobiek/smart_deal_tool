# coding=utf-8
import gevent
from gevent import monkey
monkey.patch_all(subprocess=True)
import sys
import time
import traceback
import const as ct
from log import getLogger
from creview import CReivew
from data_manager import DataManager
from new_purchase import buy_new_stock 
from cthread import CThread
log = getLogger(__name__)

def main():
    threadList = []
    dm = DataManager(ct.DB_INFO)
    threadList.append(CThread(dm.run, 0))
    threadList.append(CThread(dm.animate, 15))
    threadList.append(CThread(dm.collect, 3600))
    threadList.append(CThread(dm.update, 3600))

    cr = CReivew(ct.STAT_INFO)
    threadList.append(CThread(cr.update, 10800))

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
        sys.exit(0)
