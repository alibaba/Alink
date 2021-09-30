from __future__ import print_function
from datetime import datetime
import tensorflow as tf
import sys
import time
import json
from tensorflow.python.summary.writer.writer_cache import FileWriterCache as SummaryWriterCache
import tensorflow_on_flink.tensorflow_on_flink_ops as tff_ops
import traceback


def log_speed(steps, start):
    duration = time.time() - start
    speed = steps / duration
    print ("Read from queue: " + str(steps) + " steps, at " + '%.2f' % speed + " steps/second")
    sys.stdout.flush()


def map_fun(context):
    tf.compat.v1.disable_v2_behavior()
    print(tf.__version__)
    sys.stdout.flush()
    tf.logging.set_verbosity(tf.logging.ERROR)
    jobName = context.jobName
    index = context.index
    clusterStr = context.properties["cluster"]
    delim = context.properties["SYS:delim"]
    print (index, clusterStr)
    sys.stdout.flush()
    clusterJson = json.loads(clusterStr)
    cluster = tf.compat.v1.train.ClusterSpec(cluster=clusterJson)
    server = tf.compat.v1.train.Server(cluster, job_name=jobName, task_index=index)
    sess_config = tf.compat.v1.ConfigProto(allow_soft_placement=True, log_device_placement=False,
                                 device_filters=["/job:ps", "/job:worker/task:%d" % index])
    with tf.compat.v1.device(tf.compat.v1.train.replica_device_setter(worker_device='/job:worker/task:' + str(index), cluster=cluster)):
        dataset = context.flinkStreamDataSet(buffer_size=0)
        iterator = dataset.make_one_shot_iterator()
        input_records = iterator.get_next()

        global_step = tf.compat.v1.train.get_or_create_global_step()
        global_step_inc = tf.compat.v1.assign_add(global_step, 1)
        is_chief = (index == 0)
        print (datetime.now().isoformat() + " started ------------------------------------")
        t = time.time()
        total_step = 0
        try:
            with tf.compat.v1.train.MonitoredTrainingSession(master=server.target, is_chief=is_chief, config=sess_config,
                                                   checkpoint_dir="./target/tmp/input_output/" + str(t)) as mon_sess:
                # while not mon_sess.should_stop():
                while True:
                    total_step, _ = mon_sess.run([global_step_inc, input_records])
                    if (total_step % 10000 == 0):
                        log_speed(total_step, t)
        except Exception as e:
            print('traceback.print_exc():')
            traceback.print_exc()
            sys.stdout.flush()
        finally:
            print (datetime.now().isoformat() + " ended --------------------------------------")
            log_speed(total_step, t)
            SummaryWriterCache.clear()


if __name__ == "__main__":
    map_fun(context)
