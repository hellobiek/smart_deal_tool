import MySQLdb as db
import pandas as pd
from sqlalchemy import create_engine

conn = db.connect(host="localhost",user="root",passwd="123456",db="test",charset="utf8")
cur = conn.cursor()
cur.execute("drop table if exists user")
cur.execute('create table user(id int,name varchar(20))')
users = []
for i in range(20):
    users.append((i,"user"+str(i)))
cur.executemany("insert into user values(%s,%s)",users)
conn.commit()
conn.close()
cur.close()

#read
engine = create_engine('mysql://root:123456@localhost/test?charset=utf8')
sql = "select * from user"
df = pd.read_sql_query(sql, engine)
print df
#write
df.to_sql('user',engine,if_exists='append',index=False)
