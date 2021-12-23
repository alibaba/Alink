from __future__ import print_function
import sys
import traceback
from flink_ml_framework.java_file import JavaFile
import json
import struct


def map_func(context):

    java_file = JavaFile(context.from_java(), context.to_java())
    try:
        json_object = {'aa': 'aa'}
        json_bytes = json.dumps(json_object)
        json_len = struct.pack("<i", len(json_bytes))
        res = java_file.write(json_len, 4)
        print("res:", res)
        res = java_file.write(json_bytes, len(json_bytes))
        print("res:", res)
    except Exception as e:
        msg = traceback.format_exc()
        print (msg)
