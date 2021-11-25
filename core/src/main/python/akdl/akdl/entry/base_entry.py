import abc
from typing import Dict, Callable

import tensorflow as tf
from flink_ml_framework.context import Context
from flink_ml_framework.java_file import *

from ..runner import tf_helper, io_helper
from ..runner.output_writer import DirectOutputWriter

try:
    from flink_ml_tensorflow.tensorflow_context import TFContext
except:
    from flink_ml_tensorflow2.tensorflow_context import TFContext

try:
    # noinspection PyUnresolvedReferences
    from tensorflow_io.core.python.ops import core_ops
except:
    # For TF1.15 on Windows platform
    print("Tensorflow IO not loaded!", file=sys.stderr, flush=True)

__all__ = ['TF1_TYPE', 'TF2_TYPE']

TF1_TYPE = 'tf1'
TF2_TYPE = 'tf2'


class BaseEntry(abc.ABC):

    def __init__(self, func_name, engine_type):
        self.func_name = func_name
        self.engine_type = engine_type

    @staticmethod
    def get_func_by_name(func_name):
        """
        Get function by the func name
        :param func_name: func name
        :return: function
        """
        if '.' not in func_name:
            if func_name in globals():
                return globals()[func_name]
            else:
                raise RuntimeError('cannot find function[{}]'.format(func_name))
        else:
            module_name, func_name = func_name.rsplit('.', 1)
            import importlib
            # load the module, will raise ImportError if module cannot be loaded
            m = importlib.import_module(module_name)
            # get the class, will raise AttributeError if class cannot be found
            c = getattr(m, func_name)
            return c

    @abc.abstractmethod
    def construct_args(self, **kwargs):
        pass

    def is_batch(self):
        return True

    def post_process(self, **kwargs):
        pass

    def entry_func(self, context: Context):
        tf_context = TFContext(context)
        properties = tf_context.properties
        print('properties', properties, flush=True)

        # intra_op_parallelism is set by akdl, because there is a bug in TensorFlow 1.x
        # See: https://stackoverflow.com/questions/34426268/restricting-number-of-cores-used
        intra_op_parallelism = int(properties['ALINK:intra_op_parallelism'])
        if self.engine_type == TF1_TYPE:
            tf_helper.set_intra_op_parallelism(intra_op_parallelism_threads=intra_op_parallelism)
        elif self.engine_type == TF2_TYPE:
            tf.config.threading.set_intra_op_parallelism_threads(intra_op_parallelism)

        num_workers = int(properties['ALINK:num_workers'])
        work_dir = properties['ALINK:work_dir']
        cluster, task_type, task_index = tf_context.export_estimator_cluster()

        if self.is_batch():
            java_queue_file = JavaFile(context.from_java(), context.to_java())
            dataset_file = os.path.join(work_dir, 'dataset.tfrecords')
            dataset, dataset_length = io_helper.convert_java_queue_file_to_repeatable_dataset(java_queue_file,
                                                                                              dataset_file)
            print("number of records: " + str(dataset_length), flush=True)
            dataset_fn: Callable[[], tf.data.TFRecordDataset] = lambda: tf.data.TFRecordDataset(dataset_file)
        else:
            dataset_fn: Callable[[], tf.data.TFRecordDataset] = lambda: tf_context.flink_stream_dataset()
            dataset = None
            dataset_file = None
            dataset_length = None

        saved_model_dir = os.path.join(work_dir, 'savedmodel')

        user_params: Dict = json.loads(properties['ALINK:user_defined_params'])
        for i in range(1, 1024):
            key = "ALINK:bc_" + str(i)
            if key in properties:
                user_params[key] = context.properties[key]

        key = "ALINK:model_dir"
        if key in properties:
            user_params[key] = properties[key]

        output_writer = DirectOutputWriter(tf_context.from_java(), tf_context.to_java())

        locals_copy = locals().copy()
        locals_copy.pop("self")
        print("locals_copy = ", locals_copy, flush=True)
        args = self.construct_args(**locals_copy)

        func = self.get_func_by_name(self.func_name)
        func(args)

        print("task_type = {}, task_index = {}: done tf_user_main".format(task_type, task_index), flush=True)
        local_vars = locals().copy()
        local_vars.pop('self')
        self.post_process(**local_vars)
        print("task_type = {}, task_index = {}: exit".format(task_type, task_index), flush=True)

        output_writer.close()
