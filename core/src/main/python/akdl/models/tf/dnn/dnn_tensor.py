import json
import logging
import os
from typing import Any, List, Union, Dict

import tensorflow as tf

from akdl.engine.run_config import generate_run_config
from akdl.engine.train import train_estimator
from akdl.runner.config import TrainTaskConfig

if tf.__version__ >= '2.0':
    tf = tf.compat.v1


def dnn_tensor(task_config: TrainTaskConfig):
    user_params = task_config.user_params

    tensor_shapes = json.load(open(os.path.join(task_config.work_dir, "tensor_shapes.txt"), "r"))
    logging.info("tensor_shapes: {}".format(tensor_shapes))
    tensor_col = user_params['tensorCol']
    logging.info("tensor_col: {}".format(tensor_col))
    label_col = user_params['labelCol']
    logging.info("label_col: {}".format(label_col))

    example_config: List[Union[Dict[str, Union[str, Any]], Dict[str, Union[Union[str, List[int]], Any]]]] = [
        {
            'name': tensor_col,
            'dtype': 'float',
            'shape': tensor_shapes[tensor_col],
        },
        {
            'name': label_col,
            'dtype': 'long',
            'shape': [1]
        }
    ]

    input_config = {
        'example_config': example_config,
        'label_col': label_col
    }

    train_config = {
        'raw_dataset_fn': lambda: tf.data.TFRecordDataset(task_config.dataset_file),
        'batch_size': user_params['batch_size'],
        'num_epochs': user_params['num_epochs'],
        'log_step_count_steps': 1,
    }

    export_config = {
        'placeholders_config': [{
            'name': tensor_col,
            'dtype': 'float',
            'shape': tensor_shapes[tensor_col],
        }]
    }

    feature_columns = [
        tf.feature_column.numeric_column(tensor_col, shape=tensor_shapes[tensor_col])
    ]

    classifier = tf.estimator.DNNClassifier(
        feature_columns=feature_columns,
        hidden_units=[1024, 1024, 512, 256, 128],
        n_classes=2,
        config=generate_run_config(**train_config))

    train_estimator(classifier, input_config, train_config, export_config, task_config)
