from __future__ import print_function
import sys
import traceback
from flink_ml_framework.java_file import *


def map_func(context):

    bytes_recorder = BytesRecorder(context.from_java(), context.to_java())
    try:
        for i in range(20):
            res = bytes_recorder.write_record("a,b,c,d")
            print(context.index, "res:", res)
            sys.stdout.flush()
    except Exception as e:
        msg = traceback.format_exc()
        print (msg)
