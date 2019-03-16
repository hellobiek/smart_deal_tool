# -*- coding: utf-8 -*-
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
from ma import sma, ewma, macd
from kdj import kdj
from roc import roc
from emv import emv
from cci import cci
from obv import obv 
from boll import boll
from admi import admi
from cindex import CIndex
from force_index import force_index
import matplotlib.pyplot as plt
COLORS = ['#F5DEB3', '#A0522D', '#1E90FF', '#FFE4C4', '#00FFFF', '#DAA520', '#3CB371', '#808080', '#ADFF2F', '#4B0082']
def get_index_data(start_date, end_date, index_code):
    iobj = CIndex(index_code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    i_data = iobj.get_k_data_in_range(start_date, end_date)
    i_data = i_data[['open', 'high', 'low', 'close', 'volume', 'amount', 'date']]
    return i_data

def plot(data, keys, ilabel, has_subplot = True):
    fig = plt.figure(figsize=(9,5))
    ax = fig.add_subplot(2, 1, 1)
    ax.set_xticklabels([])
    plt.plot(data['close'], lw=1)
    plt.title('close price - %s chart' % ilabel)
    plt.ylabel('close price')
    plt.grid(True)
    if has_subplot:
        bx = fig.add_subplot(2, 1, 2)
    plt.ylabel('%s values' % ilabel)
    i = 0
    for key in keys:
        plt.plot(data[key], COLORS[i], lw = 0.75, linestyle = '-', label = key)
        i += 1
    plt.legend(loc = 2, prop = {'size':9.5})
    plt.grid(True)
    plt.show()

def plot_cci(data):
    data = cci(data, 20)
    plot(data, 'cci')

def plot_force_index(data):
    data = force_index(data, 2)
    plot(data, 'force_index')

def plot_emv(data):
    data = emv(data, 14)
    plot(data, 'emv')

def plot_roc(data):
    data = roc(data, 5)
    plot(data, ['roc'], 'roc')

def plot_kdj(data):
    data = kdj(data)
    plot(data, ["k", "d", "j"], "KDJ")

def plot_boll(data):
    n = 50
    data = boll(data, n)
    plot(data, ["upper bollingerBand", "lower bollingerBand"], "bollingerBand", False)

def plot_ewma(data):
    n = 5
    data = ewma(data, n)
    plot(data, ["ewma_5"], "ewma", False)
    
def plot_sma(data):
    n = 5
    data = sma(data, n)
    plot(data, ["sma_5"], "sma", False)

def plot_macd(data):
    data = macd(data, nslow = 26, nfast = 12)
    plot(data, ["macd", 'ewma_%s' % 12, 'ewma_%s' % 26], "macd")

def plot_obv(data):
    data = obv(data, 5)
    plot(data, ["obv_5"], "obv", True)

def plot_admi(data):
    data = admi(data, 6, 14)
    plot(data, ["adx_6_14"], "adx", True)

if __name__ == '__main__':
    index_code = '000300'
    start = '2015-10-01'
    end = '2017-11-01'
    data = get_index_data(start, end, index_code)
    #plot_kdj(data)
    #plot_cci(data)
    #plot_force_index(data)
    #plot_emv(data)
    plot_roc(data)
    #plot_boll(data)
    #plot_ewma(data)
    #plot_sma(data)
    #plot_macd(data)
    #plot_obv(data)
    #plot_admi(data)
