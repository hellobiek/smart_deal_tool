#!/usr/bin/python
# coding=utf-8
import tempfile
from crack_bmp import crack_bmp

def get_verified_code(tmp_buff):
    temp = tempfile.TemporaryFile()
    with open(temp.name, 'wb') as verify_pic:
        verify_pic.write(tmp_buff)
    return crack_bmp().decode_from_file(temp.name)
