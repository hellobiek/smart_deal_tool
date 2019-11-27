# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import os
import tempfile
import const as ct
import pandas as pd
from zipfile import ZipFile
from base.clog import getLogger
from struct import unpack, calcsize
class CReport(object):
    def __init__(self, report_dir = ct.REPORT_DIR, report_publish_dir = ct.REPORT_PUBLISH_DIR):
        self.publish_data = dict()
        self.report_dir = report_dir
        self.logger = getLogger(__name__)
        self.report_publish_dir = report_publish_dir

    def to_df(self, data):
        if len(data) == 0: return pd.DataFrame()
        total_lengh = len(data[0])
        cols = ['code', 'date']
        length = total_lengh - 2
        for i in range(0, length):
            cols.append("col" + str(i + 1))
        return pd.DataFrame(data = data, columns = cols)

    def parse(self, mdate):
        """
        获取指定日期的所有股票的财报信息
        :param mdate: yyyy-mm-dd
        :return:
        """
        item_all_list = list()
        file_name = 'gpcw%s' % mdate.replace('-', '')
        file_path = "%s/%s.zip" % (self.report_dir, file_name)
        if not os.path.isfile(file_path): return None
        with ZipFile(file_path) as myzip:
            with tempfile.TemporaryDirectory() as tmpdirname:
                header_pack_format = "<1hI1H3L"
                new_file_path = myzip.extract('%s.dat' % file_name, tmpdirname)
                with open(new_file_path, 'rb') as datfile:
                    header_size = calcsize(header_pack_format)
                    stock_item_size = calcsize("<6s1c1L")
                    data_header = datfile.read(header_size)
                    stock_header = unpack(header_pack_format, data_header)
                    report_date = stock_header[1]
                    max_count = stock_header[2]
                    report_size = stock_header[4]
                    report_fields_count = int(report_size / 4)
                    report_pack_format = "<{}f".format(report_fields_count)
                    report_pack_size = calcsize(report_pack_format)
                    for stock_idx in range(0, max_count):
                        datfile.seek(header_size + stock_idx * stock_item_size)
                        si = datfile.read(stock_item_size)
                        stock_item = unpack("<6s1c1L", si)
                        code = stock_item[0].decode("utf-8")
                        foa = stock_item[2]
                        datfile.seek(foa)
                        info_data = datfile.read(report_pack_size)
                        cw_info = unpack(report_pack_format, info_data)
                        cw_info_list = list(map((lambda x: 0.0 if x < -1000000000.0 else x), cw_info))
                        one_record = [code, report_date] + cw_info_list
                        item_all_list.append(one_record)
        return item_all_list
   
    def get_all_report_list(self):
        """
        获取所有财报信息
        """
        all_date_list = []   # 所有财报日期
        file_list = os.listdir(self.report_dir)
        for file_name in file_list:
            if file_name.startswith("."): continue
            file_name = file_name.split('.')[0]
            ymd_str = file_name[4:]
            all_date_list.append("%d-%02d-%02d" % (int(ymd_str) / 10000, int(ymd_str) % 10000 / 100, int(ymd_str) % 100))
        all_date_list.sort()
        return all_date_list

    def get_report_data(self, mdate):
        """
        获取某一季度财报的部分列
        :param mdate:季度，格式()
        :return: DataFrame()
        """
        item_list = self.parse(mdate)
        return self.to_df(item_list)

    def get_report_publish_time(self, mdate, code):
        """
        获取某一季度中, 某只股票财报信息的实际披露时间
        :param mdate:
        :param code:
        :return:
        """
        result_df = self.publish_data.get(mdate, None)
        if result_df is None or len(result_df) <= 0:
            fpath = os.path.join(self.report_publish_dir, "{}.csv".format(mdate))
            if not os.path.exists(fpath): return mdate
            self.publish_data[mdate] = pd.read_csv(fpath, header=0)
            self.publish_data[mdate]['code'] = self.publish_data[mdate]['code'].map(lambda x: str(x).zfill(6))
            self.publish_data[mdate] = self.publish_data[mdate].fillna(0)
            self.publish_data[mdate]["actual"] = self.publish_data[mdate]["actual"].astype('int')
            self.publish_data[mdate]["change"] = self.publish_data[mdate]["change"].astype('int')
            self.publish_data[mdate]["first"] = self.publish_data[mdate]["first"].astype('int')
    
        filter_df = self.publish_data[mdate][self.publish_data[mdate]['code'] == code]
        if len(filter_df) <= 0: return mdate
        for i in range(1, 4):
            if filter_df.values[0][4 - i] != 0: return filter_df.values[0][4 - i]
        return 0

if __name__ == '__main__':
    cr = CReport()
    df = cr.get_report_data('2017-09-30')
