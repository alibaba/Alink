from __future__ import print_function
import tensorflow as tf
import sys
import time
from tensorflow.python.summary.writer.writer_cache import FileWriterCache as SummaryWriterCache
from flink_ml_tensorflow2 import tensorflow_on_flink_ops as tff_ops
from flink_ml_tensorflow2.tensorflow_context import *
import traceback


def map_func(context):
    tf.compat.v1.disable_v2_behavior()
    print(tf.__version__)
    sys.stdout.flush()
    tf_context = TFContext(context)
    job_name = tf_context.get_role_name()
    index = tf_context.get_index()
    cluster_json = tf_context.get_tf_cluster()
    print (cluster_json)
    sys.stdout.flush()
    cluster = tf.compat.v1.train.ClusterSpec(cluster=cluster_json)
    server = tf.compat.v1.train.Server(cluster, job_name=job_name, task_index=index)
    sess_config = tf.compat.v1.ConfigProto(allow_soft_placement=True, log_device_placement=False,
                                 device_filters=["/job:ps", "/job:worker/task:%d" % index])
    if 'ps' == job_name:
        from time import sleep
        while True:
            sleep(1)
    else:
        with tf.compat.v1.device(tf.compat.v1.train.replica_device_setter(worker_device='/job:worker/task:' + str(index), cluster=cluster)):
            record_defaults = [[9], [tf.constant(value=9, dtype=tf.int64)], [9.0],
                               [tf.constant(value=9.0, dtype=tf.float64)], ["9.0"]]
            dataset = tf_context.flink_stream_dataset(buffer_size=0)
            dataset = dataset.map(lambda record: tf.compat.v1.decode_csv(record, record_defaults=record_defaults))
            dataset = dataset.batch(3)
            iterator = dataset.make_one_shot_iterator()
            input_records = iterator.get_next()

            global_step = tf.compat.v1.train.get_or_create_global_step()
            global_step_inc = tf.compat.v1.assign_add(global_step, 1)
            out = tff_ops.encode_csv(input_list=input_records)
            fw = tff_ops.FlinkTFRecordWriter(address=context.to_java())
            w = fw.write([out])
            is_chief = (index == 0)
            t = time.time()
            try:
                with tf.compat.v1.train.MonitoredTrainingSession(master=server.target, is_chief=is_chief, config=sess_config,
                                                       checkpoint_dir="./target/tmp/input_output/" + str(
                                                           t)) as mon_sess:
                    # while not mon_sess.should_stop():
                    while True:
                        print (index, mon_sess.run([global_step_inc, w]))
                        sys.stdout.flush()
                        #time.sleep(1)
            except Exception as e:
                print('traceback.print_exc():')
                traceback.print_exc()
                sys.stdout.flush()
                raise e
            finally:
                SummaryWriterCache.clear()


if __name__ == "__main__":
    map_fun(context)
