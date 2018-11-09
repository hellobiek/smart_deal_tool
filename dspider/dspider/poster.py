#coding=utf-8
import const as ct
from twisted.enterprise import adbapi
class Poster(object):
    def __init__(self, item):
        self.item = item.convert()

    # 使用twisted将mysql插入变成异步执行
    def post(self):
        # 指定操作方法和操作的数据
        query = self.dbpool.runInteraction(self.do_insert, self.item)
        # 指定异常处理方法
        query.addErrback(self.handle_error, self.item, spider) #处理异常

    def handle_error(self, failure, self.item, spider):
        #处理异步插入的异常
        print(failure)

    def do_insert(self, cursor, item):
        #执行具体的插入
        #根据不同的item 构建不同的sql语句并插入到mysql中
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)

class InvestorSituationItemPoster(Poster):
    def __init__(self, item):
        Poster.__init__(self, item)
        dbparms = dict(
            host        = ct.DB_INFO['host'],
            db          = 'stock',
            user        = ct.DB_INFO['user'],
            passwd      = ct.DB_INFO['password'],
            charset     = ct.UTF8,
            cursorclass = pymysql.cursors.DictCursor, 
            use_unicode = True,
        )
        self.dbpool = adbapi.ConnectionPool("pymysql", **dbparms)

class ShiborItemPoster(Poster):
    pass

class IndexCollectorItemPoster(Poster):
    pass

class IndexStatisticItemPoster(Poster):
    pass

class FoundationBriefItemPoster(Poster):
    pass
