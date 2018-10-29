#coding=utf-8
from langconv import Converter
def traditional2simplified(sentence):
    return Converter('zh-hans').convert(sentence)
