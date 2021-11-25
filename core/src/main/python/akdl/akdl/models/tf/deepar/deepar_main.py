import json
import logging
import os

import tensorflow as tf

from .deepar_model import DeepARModel, gaussian_loss
from ....engine.run_config import generate_run_config
from ....engine.train import train_estimator, train_keras_model
from ....runner.config import TrainTaskConfig

if tf.__version__ >= '2.0':
    tf = tf.compat.v1


def main(task_config: TrainTaskConfig):
    user_params = task_config.user_params

    tensor_shapes = json.load(open(os.path.join(task_config.work_dir, "tensor_shapes.txt"), "r"))
    logging.info("tensor_shapes: {}".format(tensor_shapes))
    tensor_col = user_params['tensorCol']
    logging.info("tensor_col: {}".format(tensor_col))
    label_col = user_params['labelCol']
    logging.info("label_col: {}".format(label_col))

    example_config = [
        {
            'name': tensor_col,
            'dtype': 'float',
            'shape': tensor_shapes[tensor_col],
        },
        {
            'name': label_col,
            'dtype': 'float',
            'shape': tensor_shapes[label_col]
        }
    ]

    input_config = {
        'example_config': example_config,
        'label_col': label_col
    }

    train_config = {
        'filenames': task_config.dataset_file,
        'raw_dataset_fn': lambda: tf.data.TFRecordDataset(task_config.dataset_file),
        'batch_size': int(user_params['batch_size']),
        'num_epochs': int(user_params['num_epochs']),
        'log_step_count_steps': 1,
    }

    export_config = {
        'placeholders_config': [{
            'name': tensor_col,
            'dtype': 'float',
            'shape': [None, *tensor_shapes[tensor_col][1:]],
        }],
        'export_batch_dim': True,
    }

    model_config = json.loads(user_params['model_config'])
    model_config['input_name'] = tensor_col
    model_config['input_shape'] = tensor_shapes[tensor_col]
    model_config['lstm_units'] = 4
    model_config['dense_units'] = 3

    model = DeepARModel(model_config)
    model.compile(loss=gaussian_loss, run_eagerly=None)
    estimator = tf.keras.estimator.model_to_estimator(model, config=generate_run_config(**train_config))
    # train_keras_model(model, input_config, train_config, task_config)
    train_estimator(estimator, input_config, train_config, export_config, task_config)
