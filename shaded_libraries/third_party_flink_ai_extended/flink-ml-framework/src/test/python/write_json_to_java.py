from __future__ import print_function
import sys
import traceback
from flink_ml_framework.java_file import *


def map_func(context):

    json_recorder = JsonRecorder(context.from_java(), context.to_java())
    try:
        res = json_recorder.write_record({"kk": "kkk"})
        print("res:", res)
    except Exception as e:
        msg = traceback.format_exc()
        print (msg)
