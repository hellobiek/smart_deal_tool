# -*- coding: utf-8 -*-
from array import array
class MarkdownTable(object):
    def __init__(self, headers):
        if type(headers) is not list: raise ValueError("List expected")
        self.column_num = len(headers)
        if self.column_num == 0:
            raise ValueError("Header list cannot be empty!")
        self.headers = headers
        self.column_sizes = array('i', map(len, headers))
        self.data = []

    def addRow(self, row):
        if type(row) is not list:
            raise ValueError("List expected")
        len_diff = self.column_num - len(row)
        if len_diff < 0:
            raise ValueError("Cannot have more columns than headers (%d)!" % self.column_num)
        if len_diff > 0:
            row = row + len_diff * ['']
        for index, size in enumerate(self.column_sizes):
            col_len = len(row[index])
            if col_len > self.column_sizes[index]:
                self.column_sizes[index] = col_len
        self.data.append(row)

    def getTable(self):
        out = u' | '.join(('%%-%ds' % l) % h for h, l
                          in zip(self.headers, self.column_sizes)) + '\n'
        out += u'-|-'.join('-' * l for l in self.column_sizes) + '\n'
        for row in self.data:
            out += u' | '.join(('%%-%ds' % l) % h
                               for h, l in zip(row, self.column_sizes)) + '\n'
        return out
