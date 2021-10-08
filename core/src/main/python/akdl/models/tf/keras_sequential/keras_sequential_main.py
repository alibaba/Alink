import json
import logging
import os
import time

import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1
    tf.disable_v2_behavior()

from tensorflow.keras import Model
from tensorflow.keras.layers import *
# noinspection PyUnresolvedReferences
from tensorflow.keras.optimizers import *
# noinspection PyUnresolvedReferences
import tensorflow_hub as hub
# noinspection PyUnresolvedReferences
from tensorflow_hub.keras_layer import KerasLayer

from akdl.engine.run_config import generate_run_config
from akdl.engine.train import train_estimator, train_keras_model
from akdl.runner.config import TrainTaskConfig
from akdl.runner.io_helper import remove_checkpoint_files
from typing import List


def main(task_config: TrainTaskConfig):
    user_params = task_config.user_params

    model_dir = user_params.get('model_dir', os.path.join(task_config.work_dir, "model_dir"))
    if 'ALINK:remove_checkpoint_before_training' in user_params:
        if task_config.task_type == 'chief' and task_config.task_index == 0:
            remove_checkpoint_files(model_dir)
        else:
            # TODO: wait a few seconds for the chief work to remove files
            time.sleep(5)

    tensor_shapes = json.load(open(os.path.join(task_config.work_dir, "tensor_shapes.txt"), "r"))
    logging.info("tensor_shapes: {}".format(tensor_shapes))
    tensor_col = json.loads(user_params['tensor_cols'])[0]
    logging.info("tensor_col: {}".format(tensor_col))
    label_col = user_params['label_col']
    logging.info("label_col: {}".format(label_col))
    label_type_str = user_params['label_type']

    if 'ALINK:bc_1' in user_params:
        with open(user_params['ALINK:bc_1'], 'r') as f:
            num_classes = int(f.readline().strip())
    else:
        num_classes = 1
    logging.info("num_classes: {}".format(num_classes))

    model_config = json.loads(user_params['model_config'])
    layers_str = model_config['layers']

    example_config = [
        {
            'name': tensor_col,
            'dtype': 'double',
            'shape': tensor_shapes[tensor_col],
        },
        {
            'name': label_col,
            'dtype': label_type_str,
            'shape': []
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
            'dtype': 'double',
            'shape': tensor_shapes[tensor_col],
        }],
        'export_batch_dim': True
    }

    model = get_model(tensor_name=tensor_col, tensor_shape=tensor_shapes[tensor_col],
                      layers_str=layers_str,
                      num_classes=num_classes)
    model.summary()

    if num_classes == 1:
        loss = tf.keras.losses.MeanAbsoluteError()
        metrics = ['mse']
    else:
        loss = tf.keras.losses.SparseCategoricalCrossentropy()
        metrics = ['accuracy']

    optimizer = eval(user_params.get('optimizer', 'Adam()'))

    model.compile(optimizer=optimizer, loss=loss, metrics=metrics)
    estimator = tf.keras.estimator.model_to_estimator(model,
                                                      model_dir=model_dir,
                                                      config=generate_run_config(**train_config))
    train_estimator(estimator, input_config, train_config, export_config, task_config)
    # train_keras_model(model, input_config, train_config, task_config)


def get_model(tensor_name, tensor_shape, layers_str: List[str], num_classes: int):
    inputs = Input(shape=tensor_shape, name=tensor_name)
    x = inputs
    for layer_str in layers_str:
        x = eval(layer_str)(x)
    if num_classes == 1:
        y = Dense(1, name='y')(x)
        return Model(inputs, y)
    else:
        logits = Dense(num_classes, name='logits')(x)
        probs = Softmax(name='probs')(logits)
        return Model(inputs, probs)
