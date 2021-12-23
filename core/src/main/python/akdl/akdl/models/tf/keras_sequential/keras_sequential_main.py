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
from akdl.engine.train import train_estimator, train_keras_model, train_estimator_one_worker
from akdl.runner.config import TrainTaskConfig
from akdl.runner.io_helper import remove_checkpoint_files
from akdl.models.tf.keras_sequential.metrics_from_logits import AUC, BinaryAccuracy, TruePositives, FalsePositives, TrueNegatives, FalseNegatives, Precision, Recall
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

    # input config
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
    logging.info("input_config = {}".format(input_config))

    # train config
    raw_dataset_fn = lambda: tf.data.TFRecordDataset(task_config.dataset_file)
    valid_raw_dataset_fn = None
    train_dataset_length = task_config.dataset_length

    only_one_worker = user_params.get('ALINK:ONLY_ONE_WORKER', 'false') == 'true'
    if only_one_worker:
        os.environ.pop('TF_CONFIG', None)
        local_valid_ratio = float(user_params.get('validation_split', 0.))
        valid_dataset_length = int(task_config.dataset_length * local_valid_ratio)
        logging.info("valid_dataset_length: {}".format(valid_dataset_length))
        raw_dataset_fn = lambda: tf.data.TFRecordDataset(task_config.dataset_file).skip(valid_dataset_length)
        valid_raw_dataset_fn = lambda: tf.data.TFRecordDataset(task_config.dataset_file).take(valid_dataset_length)
        train_dataset_length = train_dataset_length - valid_dataset_length

    batch_size = int(user_params['batch_size'])
    num_epochs = int(user_params['num_epochs'])
    save_checkpoints_epochs = float(user_params.get('save_checkpoints_epochs', 1.))
    train_config = {
        'filenames': task_config.dataset_file,
        'raw_dataset_fn': raw_dataset_fn,
        'batch_size': batch_size,
        'num_epochs': num_epochs,
        'log_step_count_steps': 1
    }
    if valid_raw_dataset_fn is not None:
        train_config.update({
            'valid_raw_dataset_fn': valid_raw_dataset_fn,
        })
    if 'save_checkpoints_secs' in user_params:
        train_config.update({
            'save_checkpoints_secs': float(user_params['save_checkpoints_secs'])
        })
    else:
        train_config.update({
            'save_checkpoints_steps': train_dataset_length * save_checkpoints_epochs / batch_size,
        })
    logging.info("train_config = {}".format(train_config))

    # export config
    save_best_only = user_params.get('save_best_only') == 'true'
    best_exporter_metric = user_params.get('best_exporter_metric', 'loss')
    metric_bigger = best_exporter_metric in [
        'acc', 'auc', 'binary_accuracy', 'precision', 'recall', 'true_negatives', 'true_positives'
    ]
    export_config = {
        'placeholders_config': [{
            'name': tensor_col,
            'dtype': 'double',
            'shape': tensor_shapes[tensor_col],
        }],
        'export_batch_dim': True,
        'exporter_type': 'best' if save_best_only else 'latest',
        'best_exporter_metric': best_exporter_metric,
        'metric_bigger': metric_bigger,
    }
    logging.info("export_config = {}".format(export_config))

    model = get_model(tensor_name=tensor_col, tensor_shape=tensor_shapes[tensor_col],
                      layers_str=layers_str,
                      num_classes=num_classes)
    model.summary(print_fn=lambda d: logging.info(d))

    if num_classes == 1:
        loss = tf.keras.losses.MeanAbsoluteError()
        metrics = ['mse', tf.keras.metrics.RootMeanSquaredError(), 'mae', 'mape', 'msle']
    elif num_classes == 2:
        loss = tf.keras.losses.BinaryCrossentropy(from_logits=True)
        metrics = ['accuracy',
                   AUC(from_logits=True),
                   BinaryAccuracy(from_logits=True),
                   TruePositives(from_logits=True),
                   FalsePositives(from_logits=True),
                   TrueNegatives(from_logits=True),
                   FalseNegatives(from_logits=True),
                   Precision(from_logits=True),
                   Recall(from_logits=True)
                   ]
    else:
        loss = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
        metrics = ['accuracy']

    optimizer = eval(user_params.get('optimizer', 'Adam()'))

    model.compile(optimizer=optimizer, loss=loss, metrics=metrics)
    estimator = tf.keras.estimator.model_to_estimator(model,
                                                      model_dir=model_dir,
                                                      config=generate_run_config(**train_config))

    if not only_one_worker:
        train_estimator(estimator, input_config, train_config, export_config, task_config)
    else:
        train_estimator_one_worker(estimator, input_config, train_config, export_config, task_config)
    # train_keras_model(model, input_config, train_config, task_config)


def get_model(tensor_name, tensor_shape, layers_str: List[str], num_classes: int):
    inputs = Input(shape=tensor_shape, name=tensor_name)
    x = inputs
    for layer_str in layers_str:
        x = eval(layer_str)(x)
    if num_classes == 1:
        y = Dense(1, name='y')(x)
        return Model(inputs, y)
    elif num_classes == 2:
        logits = Dense(1, name='logits')(x)
        return Model(inputs, logits)
    else:
        logits = Dense(num_classes, name='logits')(x)
        return Model(inputs, logits)
