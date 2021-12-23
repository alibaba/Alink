import os

import tensorflow as tf

from akdl.runner import io_helper

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

from akdl.runner.config import TrainTaskConfig, BatchTaskConfig
from . import dnn_train


def main(task_config: BatchTaskConfig):
    saved_model_dir = os.path.join(task_config.work_dir, 'saved_model_dir')
    train_task_config = TrainTaskConfig(saved_model_dir=saved_model_dir,
                                        **task_config.__dict__)
    dnn_train.main(train_task_config)

    task_type = task_config.task_type
    task_index = task_config.task_index
    output_writer = task_config.output_writer
    if task_type == 'chief' and task_index == 0:
        print("task_type = {}, task_index = {}: before output_model_to_flink".format(task_type, task_index),
              flush=True)
        io_helper.output_model_to_flink(saved_model_dir, output_writer)
        print("task_type = {}, task_index = {}: after output_model_to_flink".format(task_type, task_index),
              flush=True)
