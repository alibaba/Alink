from __future__ import print_function
import sys
import traceback
from flink_ml_framework.java_file import *


def map_func(context):

    bytes_recorder = BytesRecorder(context.from_java(), context.to_java())
    try:
        res = bytes_recorder.write_record(bytes("aaaaaaa", 'iso-8859-1'))
        print("res:", res)
    except Exception as e:
        msg = traceback.format_exc()
        print (msg)
