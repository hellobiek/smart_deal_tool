#coding=utf-8
import abc
class CClass(metaclass = abc.ABCMeta):
    @property
    @abc.abstractmethod
    def dbname(code):
        raise NotImplementedError()

    @dbname.setter
    @abc.abstractmethod
    def dbname(self, value):
        raise NotImplementedError()

    @abc.abstractmethod
    def create_db(self, db_name):
        raise NotImplementedError()

    @abc.abstractmethod
    def is_table_exists(self, table_name):
        raise NotImplementedError()

    @abc.abstractmethod
    def is_date_exists(self, table_name, cdate):
        raise NotImplementedError()
