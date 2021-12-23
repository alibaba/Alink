from __future__ import print_function
import sys
import traceback
import struct
from flink_ml_framework.java_file import JavaFile


def map_func(context):
    java_file = JavaFile(context.from_java(), context.to_java())
    try:
        res = java_file.read(4)
        # len = int(''.join(reversed(res)).encode('hex'), 16)
        data_len, = struct.unpack("<i", res)
        print("res", type(data_len), data_len)
        data = java_file.read(data_len)
        print("data", str(data))
    except Exception as e:
        msg = traceback.format_exc()
        print (msg)
