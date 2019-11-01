#-*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import struct
import const as ct
import pandas as pd
from ctypes import *
### take ref this article :http://blog.csdn.net/fangle6688/article/details/50956609
### and this http://blog.sina.com.cn/s/blog_6b2f87db0102uxo3.html
class GbbqReader(object):
    def get_df(self, fname):
        if sys.version_info.major == 2:
            bin_keys = bytearray.fromhex(ct.GBHQ_HEXDUMP_KEYS)
        else:
            bin_keys = bytes.fromhex(ct.GBHQ_HEXDUMP_KEYS)
        result = []
        with open(fname, "rb") as f:
            content = f.read()
            pos = 0
            (count, ) = struct.unpack("<I", content[pos: pos+4])
            pos += 4
            encrypt_data = content
            # data_len = len(encrypt_data)
            data_offset = pos

            for _ in range(count):
                clear_data = bytearray()
                for i in range(3):
                    (eax, ) = struct.unpack("<I", bin_keys[0x44: 0x44 + 4])
                    (ebx, ) = struct.unpack("<I", encrypt_data[data_offset: data_offset+4])
                    num = c_uint32(eax ^ ebx).value
                    (numold, ) = struct.unpack("<I", encrypt_data[data_offset + 0x4: data_offset + 0x4 + 4])
                    for j in reversed(range(4, 0x40+4, 4)):
                        ebx = (num & 0xff0000) >> 16
                        (eax, ) = struct.unpack("<I", bin_keys[ebx * 4 + 0x448: ebx * 4 + 0x448 + 4])
                        ebx = num >> 24
                        (eax_add, ) = struct.unpack("<I", bin_keys[ebx * 4 + 0x48: ebx * 4 + 0x48 + 4])
                        eax += eax_add
                        eax = c_uint32(eax).value
                        ebx = (num & 0xff00) >> 8
                        (eax_xor, ) = struct.unpack("<I", bin_keys[ebx * 4 + 0x848: ebx * 4 + 0x848 + 4])
                        eax ^= eax_xor
                        eax = c_uint32(eax).value
                        ebx = num & 0xff
                        (eax_add, ) = struct.unpack("<I", bin_keys[ebx * 4 + 0xC48: ebx * 4 + 0xC48 + 4])
                        eax += eax_add
                        eax = c_uint32(eax).value
                        (eax_xor, ) = struct.unpack("<I", bin_keys[j: j + 4])
                        eax ^= eax_xor
                        eax = c_uint32(eax).value
                        ebx = num
                        num = numold ^ eax
                        num = c_uint32(num).value
                        numold = ebx

                    (numold_op, ) = struct.unpack("<I", bin_keys[0: 4])
                    numold ^= numold_op
                    numold = c_uint32(numold).value
                    clear_data.extend(struct.pack("<II", numold, num))
                    data_offset += 8

                clear_data.extend(encrypt_data[data_offset: data_offset+5])

                (v1,v2, v3,v4,v5,v6,v7,v8) = (struct.unpack("<B7sIBffff", clear_data))
                line = (v1,
                        v2.rstrip(b"\x00").decode("utf-8"),
                        v3,
                        v4,
                        v5,
                        v6,
                        v7,
                        v8)
                result.append(line)
                data_offset += 5
        df = pd.DataFrame(data=result, columns=['market', 'code', 'datetime', 'category', 'hongli_panqianliutong',\
                                                'peigujia_qianzongguben', 'songgu_qianzongguben', 'peigu_houzongguben'])
        return df

if __name__ == '__main__':
    df = GbbqReader().get_df("/tongdaxin/T0002/hq_cache/gbbq")
    print(df)
