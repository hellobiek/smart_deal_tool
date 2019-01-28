#-*- coding: utf-8 -*-
#https://mp.weixin.qq.com/s/neCSaWK0c4jzWwCfDVFA6A
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
import numpy as np
import pandas as pd
from cstock import CStock
import matplotlib.pyplot as plt

start_date = '2016-01-01'
end_date = '2019-01-01' 
selected = ['601318', '600547', '002153', '600584', '601933', '600600', '000063', '000837', '000543', '000423']

df = pd.DataFrame()
for code in selected:
    obj = CStock(code, dbinfo = ct.OUT_DB_INFO, redis_host = '127.0.0.1')
    data = obj.get_k_data_in_range(start_date, end_date)
    data['code'] = code
    data = data[['code', 'date', 'close']]
    df = df.append(data)

df = df.set_index('date')
table = df.pivot(columns='code')
table = table[~table.index.isin(table[table.isnull().any(axis=1)].index.tolist())]

returns_daily  = table.pct_change()
returns_annual = returns_daily.mean() * 250

cov_daily  = returns_daily.cov()
cov_annual = cov_daily * 250

port_returns = []
port_volatility = []
sharpe_ratio = []
stock_weights = []

num_assets = len(selected)
num_portfolios = 500000

np.random.seed(101)
for single_portfolio in range(num_portfolios):
    weights = np.random.random(num_assets)
    weights /= np.sum(weights)
    returns = np.dot(weights, returns_annual)
    volatility = np.sqrt(np.dot(weights.T, np.dot(cov_annual, weights)))
    sharpe = returns / volatility
    sharpe_ratio.append(sharpe)
    port_returns.append(returns)
    port_volatility.append(volatility)
    stock_weights.append(weights)

portfolio = {'Returns': port_returns,
             'Volatility': port_volatility,
             'Sharpe Ratio': sharpe_ratio}

for counter,symbol in enumerate(selected):
    portfolio[symbol+' Weight'] = [Weight[counter] for Weight in stock_weights]

df = pd.DataFrame(portfolio)
column_order = ['Returns', 'Volatility', 'Sharpe Ratio'] + [stock+' Weight' for stock in selected]
df = df[column_order]

min_volatility = df['Volatility'].min()
max_sharpe = df['Sharpe Ratio'].max()

sharpe_portfolio = df.loc[df['Sharpe Ratio'] == max_sharpe]
min_variance_port = df.loc[df['Volatility'] == min_volatility]

plt.style.use('seaborn-dark')
df.plot.scatter(x='Volatility', y='Returns', c='Sharpe Ratio', cmap='RdYlGn', edgecolors='black', figsize=(10, 8), grid=True)
plt.scatter(x=sharpe_portfolio['Volatility'], y=sharpe_portfolio['Returns'], c='red', marker='D', s=200)
plt.scatter(x=min_variance_port['Volatility'], y=min_variance_port['Returns'], c='blue', marker='D', s=200 )
plt.xlabel('Volatility (Std. Deviation)')
plt.ylabel('Expected Returns')
plt.title('Efficient Frontier')
plt.show()
