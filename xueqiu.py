#coding=utf-8
import json
import easytrader

user = easytrader.use('xq')
user.prepare(config_file = 'data/xueqiu.json')

print(json.dumps(user.balance,ensure_ascii=False))
print(json.dumps(user.balance[0]['enable_balance'],ensure_ascii=False))
print(json.dumps(user.position,ensure_ascii=False))
print(json.dumps(user.position[0]['enable_amount'],ensure_ascii=False))
