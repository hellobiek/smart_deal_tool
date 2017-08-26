import MySQLdb
try:
    conn = MySQLdb.connect(host='localhost',user='root',passwd='123456',db='test',charset='utf8')
    cur = conn.cursor()
    cur.execute('drop table user')
    cur.execute('create table user(id int,name varchar(20))')
    users = []
    for i in range(20):
        users.append((i,"user"+str(i)))
    cur.executemany("insert into user values(%s,%s)",users)
    cur.execute("update user set name=\"test\" where id=2")
    res = cur.fetchone()
    print res
    res = cur.fetchmany(10)
    print res
    print cur.fetchall()
    conn.commit()
    cur.execute("select * from user" )
    cur.close()
    conn.close()
except MySQLdb.Error,e:
     print "Mysql Error %d: %s" % (e.args[0], e.args[1])
