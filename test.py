# encoding: utf-8
from gevent import monkey
monkey.patch_all()
import socket
import gevent
import multiprocessing as mp
NUM = 2

def work(i):
    urls = ['www.baidu.com', 'www.gevent.org', 'www.python.org']
    jobs = [gevent.spawn(func1, url, i) for url in urls]
    gevent.joinall(jobs)
    print("{} Done".format(mp.current_process().name))

def func1(url, i):
    print("Start Gevent: {} for Process {}".format(url, i))
    res = socket.gethostbyname(url)
    print("End Gevent: {} for {} for Process {}".format(url, res, i))

def main():
    processes = [mp.Process(name="Process-{}".format(i), target=work, args=(i,)) for i in range(NUM)]
    for process in processes:
        process.start()

    for process in processes:
        process.join()

if __name__ == '__main__':
    main()
