import tensorflow as tf
import sys
import time
import json
from tensorflow.python.summary.writer.writer_cache import FileWriterCache as SummaryWriterCache
import tensorflow_on_flink.tensorflow_on_flink_ops as tff_ops
from flink_ml_tensorflow2.tensorflow_context import TFContext

def map_fun(context):
    tf.compat.v1.disable_v2_behavior()
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

            global_step = tf.compat.v1.train.get_or_create_global_step()
            global_step_inc = tf.compat.v1.assign_add(global_step, 1)
            input_records = [tf.constant([1, 2, 3]),
                             tf.constant([1.0, 2.0, 3.0]), tf.constant(['1.0', '2.0', '3.0'])]
            out = tff_ops.encode_csv(input_list=input_records, field_delim='|')
            fw = tff_ops.FlinkTFRecordWriter(address=context.toFlink())
            w = fw.write([out])
            is_chief = (index == 0)
            t = time.time()
            try:
                hooks = [tf.compat.v1.train.StopAtStepHook(last_step=50)]
                with tf.compat.v1.train.MonitoredTrainingSession(master=server.target, config=sess_config, is_chief=is_chief,
                                                       checkpoint_dir="./target/tmp/with_output/"+str(t), hooks=hooks) as mon_sess:
                    while not mon_sess.should_stop():
                        print (index, mon_sess.run([global_step_inc, w]))
                        sys.stdout.flush()
                        time.sleep(1)
            finally:
                SummaryWriterCache.clear()


if __name__ == "__main__":
    map_fun(context)
