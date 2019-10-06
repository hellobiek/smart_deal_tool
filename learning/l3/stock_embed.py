# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from cstock import CStock
from common import get_chinese_font
from matplotlib.collections import LineCollection
from sklearn import cluster, covariance, manifold
from algotrade.model.follow_trend import FollowTrendModel
# #############################################################################
# Retrieve the data from Internet
# The data is from 2003 - 2008. This is reasonably calm: (not too long ago so
# that we get high-tech firms, and before the 2008 crash). This kind of
# historical data can be obtained for from APIs like the quandl.com and
# alphavantage.co ones.
start_date = '2018-11-01'
end_date   = '2019-08-01'
redis_host = "127.0.0.1"
dbinfo = ct.OUT_DB_INFO
report_dir = "/Volumes/data/quant/stock/data/tdx/report"
cal_file_path = "/Volumes/data/quant/stock/conf/calAll.csv"
stocks_dir = "/Volumes/data/quant/stock/data/tdx/history/days"
bonus_path = "/Volumes/data/quant/stock/data/tdx/base/bonus.csv"
rvaluation_dir = "/Volumes/data/quant/stock/data/valuation/rstock"
base_stock_path = "/Volumes/data/quant/stock/data/tdx/history/days"
valuation_path = "/Volumes/data/quant/stock/data/valuation/reports.csv"
pledge_file_dir = "/Volumes/data/quant/stock/data/tdx/history/weeks/pledge"
report_publish_dir = "/Volumes/data/quant/stock/data/crawler/stock/financial/report_announcement_date"
ftm = FollowTrendModel(valuation_path, bonus_path, stocks_dir, base_stock_path, report_dir, report_publish_dir, pledge_file_dir, rvaluation_dir, cal_file_path, dbinfo = dbinfo, redis_host = redis_host, should_create_mysqldb = False)

data = ftm.get_stock_pool(end_date)
trading_dates = ftm.cal_client.trading_day_series(start_date, end_date)
trading_day_num = len(trading_dates)
data = data[['code', 'name']]
code_list = data['code'].tolist()
name_list = data['name'].tolist()
code2namedict = dict(zip(code_list, name_list))

quotes = []
codes, names = np.array(sorted(code2namedict.items())).T
for code in codes:
    df = CStock(code).get_k_data()
    df = df.loc[df.date > start_date]
    df = df.reset_index(drop = True)
    df['code'] = code
    df = df.loc[1:trading_day_num]
    if len(df) != trading_day_num:
        raise Exception("length of df {} is not equal to {}".format(len(df), trading_day_num))
    quotes.append(df)

close_prices = np.vstack([q['close'] for q in quotes])
open_prices = np.vstack([q['open'] for q in quotes])

# The daily variations of the quotes are what carry most information
variation = close_prices - open_prices

# #############################################################################
# Learn a graphical structure from the correlations
edge_model = covariance.GraphicalLassoCV(cv=5)

# standardize the time series: using correlations rather than covariance
# is more efficient for structure recovery
X = variation.copy().T
X /= X.std(axis=0)
edge_model.fit(X)

# #############################################################################
# Cluster using affinity propagation

_, labels = cluster.affinity_propagation(edge_model.covariance_)
n_labels = labels.max()
for i in range(n_labels + 1):
    print('Cluster %i: %s' % ((i + 1), ', '.join(names[labels == i])))

# #############################################################################
# Find a low-dimension embedding for visualization: find the best position of
# the nodes (the stocks) on a 2D plane

# We use a dense eigen_solver to achieve reproducibility (arpack is
# initiated with random vectors that we don't control). In addition, we
# use a large number of neighbors to capture the large-scale structure.
node_position_model = manifold.LocallyLinearEmbedding(n_components=2, eigen_solver='dense', n_neighbors=6)
embedding = node_position_model.fit_transform(X.T).T
# #############################################################################
# Visualization
plt.figure(1, facecolor='w', figsize=(10, 8))
plt.clf()
ax = plt.axes([0., 0., 1., 1.])
plt.axis('off')

# Display a graph of the partial correlations
partial_correlations = edge_model.precision_.copy()
d = 1 / np.sqrt(np.diag(partial_correlations))
partial_correlations *= d
partial_correlations *= d[:, np.newaxis]
non_zero = (np.abs(np.triu(partial_correlations, k=1)) > 0.02)

# Plot the nodes using the coordinates of our embedding
plt.scatter(embedding[0], embedding[1], s=100 * d ** 2, c=labels, cmap=plt.cm.nipy_spectral)

# Plot the edges
start_idx, end_idx = np.where(non_zero)
# a sequence of (*line0*, *line1*, *line2*), where::
#            linen = (x0, y0), (x1, y1), ... (xm, ym)
segments = [[embedding[:, start], embedding[:, stop]]
            for start, stop in zip(start_idx, end_idx)]
values = np.abs(partial_correlations[non_zero])
lc = LineCollection(segments, zorder=0, cmap=plt.cm.hot_r, norm=plt.Normalize(0, .7 * values.max()))
lc.set_array(values)
lc.set_linewidths(15 * values)
ax.add_collection(lc)

# Add a label to each node. The challenge here is that we want to
# position the labels to avoid overlap with other labels
for index, (name, label, (x, y)) in enumerate(zip(names, labels, embedding.T)):
    dx = x - embedding[0]
    dx[index] = 1
    dy = y - embedding[1]
    dy[index] = 1
    this_dx = dx[np.argmin(np.abs(dy))]
    this_dy = dy[np.argmin(np.abs(dx))]
    if this_dx > 0:
        horizontalalignment = 'left'
        x = x + .002
    else:
        horizontalalignment = 'right'
        x = x - .002
    if this_dy > 0:
        verticalalignment = 'bottom'
        y = y + .002
    else:
        verticalalignment = 'top'
        y = y - .002
    plt.text(x, y, name, size=10, horizontalalignment=horizontalalignment, verticalalignment=verticalalignment,
            bbox=dict(facecolor='w', edgecolor=plt.cm.nipy_spectral(label / float(n_labels)), alpha=.6), fontproperties = get_chinese_font("OUT"))

plt.xlim(embedding[0].min() - .15 * embedding[0].ptp(), embedding[0].max() + .10 * embedding[0].ptp(),)
plt.ylim(embedding[1].min() - .03 * embedding[1].ptp(), embedding[1].max() + .03 * embedding[1].ptp())
plt.show()
