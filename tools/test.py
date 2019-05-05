# encoding: utf-8
import os
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import const as ct
from cstock_info import CStockInfo
with open('result', 'r') as f:
    out = f.read()

cs = CStockInfo(ct.OUT_DB_INFO, redis_host = '127.0.0.1')
info = cs.get(redis = cs.redis)
info = info[['code', 'name']]
adict = dict()
for row in out.split():
    [name, reson] = row.split(':')
    x = info.loc[info.name==name]
    if not x.empty:
        code = x['code'].values[0]
    else:
        print("%s not found" % name)
    adict[code] = row

xdict = ct.BLACK_DICT
newdict = {**xdict, **adict}

import json
jsonDumpsIndentStr = json.dumps(newdict, indent=1, ensure_ascii=False)
print(jsonDumpsIndentStr)
