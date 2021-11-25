import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1
    gfile = tf.compat.v1.gfile
    from tensorflow.core.protobuf import config_pb2

    ConfigProto = config_pb2.ConfigProto
    GPUOptions = config_pb2.GPUOptions
else:
    gfile = tf.gfile
    GPUOptions = tf.GPUOptions
    ConfigProto = tf.ConfigProto


def set_intra_op_parallelism(intra_op_parallelism_threads=1):
    """
    To set intra_op_parallelism_threads and inter_op_parallelism_threads

    The intra_op_parallelism_threads will be used for the Eigen thread pool
    which is always global, thus subsequent sessions will not influence this anymore.

    We have to set the parallelism very early in the python script.

    See: https://stackoverflow.com/questions/34426268/restricting-number-of-cores-used
    """
    gpu_options = GPUOptions(allow_growth=True)
    session_config = tf.ConfigProto(
        gpu_options=gpu_options,
        inter_op_parallelism_threads=1,
        intra_op_parallelism_threads=intra_op_parallelism_threads)
    var_a = tf.constant([[1.0, 1.0], [1.0, 1.0]])
    var_b = tf.constant([1.0, 1.0])
    var_c = var_a * var_b
    with tf.Session(config=session_config) as sess:
        sess.run(var_c)


def preview_tfrecords_file(filepath, count=None):
    for index, serialized_example in enumerate(tf.python_io.tf_record_iterator(filepath)):
        print(index, tf.train.Example.FromString(serialized_example))
        if count is not None and index >= count:
            break
