from __future__ import print_function
import sys
import time
from flink_ml_tensorflow.tensorflow_context import TFContext

def map_func(context):
    tf_context = TFContext(context)
    job_name = tf_context.get_role_name()
    index = tf_context.get_index()
    cluster_json = tf_context.get_tf_cluster()
    print (cluster_json)
    sys.stdout.flush()
    if "worker" == job_name and 0 == index:
        time.sleep(3)
        print("worker 0 finish!")
        sys.stdout.flush()
    else:
        while True:
            print("hello world!")
            sys.stdout.flush()
            time.sleep(3)


if __name__ == "__main__":
    pass
