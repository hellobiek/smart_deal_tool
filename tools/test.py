# encoding: utf-8
import const as ct
from cstock_info import CStockInfo
with open('result', 'r') as f:
    out = f.read()

#cs = CStockInfo(ct.OUT_DB_INFO, redis_host = '127.0.0.1')
#info = cs.get(redis = cs.redis)
#info = info[['code', 'name']]
adict = dict()
for row in out.split():
    code  = row[0:6]
    name = row[6:]
    reason = "%s: 配资黑名单" % name
    adict[code] = reason

import json
jsonDumpsIndentStr = json.dumps(adict, indent=1, ensure_ascii=False)
print(jsonDumpsIndentStr)
