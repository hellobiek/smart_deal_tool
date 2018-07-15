#!/usr/local/bin/python3
# coding:utf-8
import os
from PIL import Image, ImageEnhance, ImageFilter
import sys
from num_data import *

class crack_bmp:
    def __init__(self):
        self.tmp = ""

    def get_similarty(self, num, matrix):
        similar_num = 0
        for i in range(4):
            new_num = []
            for j in range(len(num)):
                now_col = j % 13
                if now_col >= i and now_col < 9 + i:
                    new_num.append(num[j])

            # print "new_num=", len(new_num), ", matrix_len=", len(matrix)
            tmp_similar_num = 0
            for j in range(len(new_num)):
                if new_num[j] == matrix[j]:
                    tmp_similar_num += 1
            # print "tmp_similar_num = ", tmp_similar_num
            if tmp_similar_num > similar_num:
                similar_num = tmp_similar_num

        return similar_num

    def decode_from_file(self, file_path):
        im = Image.open(file_path)
        #print im.format, im.size, im.mode
        # im = im.filter(ImageFilter.MedianFilter())
        enhancer = ImageEnhance.Contrast(im)
        im = enhancer.enhance(4)
        im = im.convert("1")
        valueList = list(im.getdata())
        begin_row = 3  # 前后几行都是空白，没用
        end_row = 15
        begin_col = 6  # 左边后右边也是空白
        end_col = 57
        nums = []  # 记录四个数字
        n = 4
        m = 169  # 切13*13的字出来
        nums = [[] for i in range(4)]

        total_row = 20  # 图片的行数
        total_col = 60  # 图片的列数

        # 切割数组，弄出4个13*13的一维数组
        for i in range(0, len(valueList)):
            val = valueList[i]
            row = i / total_col
            col = i % total_col
            num_index = (col - 6) / 13
            #if i % total_col == 0 and i / total_col > 0:
            #    print "\n",
            if row < begin_row or row > end_row or col < begin_col or col > end_col:
                continue
            elif val == 255:
                nums[num_index].append(0)
                #print "_",
            elif valueList[i - total_col] == 255 and valueList[i - 1] == 255 and valueList[i + 1] == 255 and valueList[
                        i + total_col] == 255:
                nums[num_index].append(0)
                #print "_",
            else:
                nums[num_index].append(1)
                #print "1",
        # 解析这4个数组，得到数字
        code_str = ""
        for num in nums:
            max_similarity_index = 0
            max_similarity = 0
            for i in range(len(num_matrix)):
                tmp_similarity = self.get_similarty(num, num_matrix[i])
                # print "i =", i, ", tmp_similarity=", tmp_similarity
                if tmp_similarity >= max_similarity:
                    max_similarity_index = i
                    max_similarity = tmp_similarity
            # print max_similarity_index
            code_str = code_str + str(max_similarity_index)
        return code_str
