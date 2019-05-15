# coding=utf-8
from gevent import monkey
monkey.patch_all()
import time
import const as ct
from base.clog import getLogger 
from base.cthread import CThread
from broker.ctrader import CTrader
from data_manager import DataManager
log = getLogger(__name__)
def main():
    time.sleep(200)
    threadList = []
    dm = DataManager(ct.DB_INFO)
    ctrader = CTrader(ct.DB_INFO)
    log.info("init succeed")
    #threadList.append(CThread(dm.run, 600))
    threadList.append(CThread(dm.update, 1800))
    threadList.append(CThread(ctrader.buy_new_stock, 3600))

    for thread in threadList:
        thread.start()

    for thread in threadList:
        thread.join()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(e)
