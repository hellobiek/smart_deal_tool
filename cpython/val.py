#coding=utf-8
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
from cval import CValuation
if __name__ == '__main__':
    val_client = CValuation()
    val_client.convert()
