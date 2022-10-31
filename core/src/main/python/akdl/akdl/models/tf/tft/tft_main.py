import json
import logging
import os
import sys

import tensorflow as tf
from akdl.engine.run_config import generate_run_config
from akdl.engine.train import train_estimator, train_estimator_one_worker
from akdl.models.tf.tft.data_formatters.base import DataTypes, InputTypes

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

from .libs.tft_model import TemporalFusionTransformer
from ....runner.config import TrainTaskConfig
from .data_formatters.base import GenericDataFormatter


class AlinkDataFormatter(GenericDataFormatter):

    def set_scalers(self, df):
        raise NotImplementedError()

    def transform_inputs(self, df):
        raise NotImplementedError()

    def format_predictions(self, df):
        raise NotImplementedError()

    def split_data(self, df):
        raise NotImplementedError()

    @property
    def _column_definition(self):
        return self._column_definition_

    def get_fixed_params(self):
        return self._fixed_params

    def __init__(self, column_definition, fixed_params, model_params):
        self._column_definition_ = column_definition
        self._fixed_params = fixed_params
        self._model_params = model_params
        self._num_classes_per_cat_input = None


def main(task_config: TrainTaskConfig):
    user_params = task_config.user_params

    print(f"user_params = {json.dumps(user_params)}")

    tensor_shapes = json.load(open(os.path.join(task_config.work_dir, "tensor_shapes.txt"), "r"))
    logging.info("tensor_shapes: {}".format(tensor_shapes))
    tensor_col = user_params['tensorCol']
    logging.info("tensor_col: {}".format(tensor_col))
    label_col = user_params['labelCol']
    logging.info("label_col: {}".format(label_col))

    gpu_devices = json.loads(user_params['ALINK:gpu_devices'])
    logging.info("gpu_devices: {}".format(gpu_devices))

    fixed_params = json.loads(user_params['fixed_params'])
    print(f'fixed_params = {fixed_params}', flush=True)

    example_config = [
        {
            'name': tensor_col,
            'dtype': 'float',
            'shape': tensor_shapes[tensor_col][1:],
            'feature_type': tf.FixedLenSequenceFeature
        },
        {
            'name': label_col,
            'dtype': 'float',
            'shape': tensor_shapes[label_col][1:],
            'feature_type': tf.FixedLenSequenceFeature
        }
    ]

    input_config = {
        'example_config': example_config,
        'label_col': label_col
    }

    def expand_raw_time_series(raw_tensors_dict, raw_labels, total_time_steps: int, lags: int):
        raw_inputs = raw_tensors_dict['inputs']
        print(f'raw_inputs = {raw_inputs}')
        print(f'raw_labels = {raw_labels}')
        t = tf.shape(raw_inputs)[0]
        inputs = tf.map_fn(lambda i: raw_inputs[i: i + total_time_steps, :],
                           tf.range(t - total_time_steps + 1),
                           dtype=tf.float32)
        inputs = tf.ensure_shape(inputs, [None, total_time_steps, None])
        print(f'inputs = {inputs}')
        labels = tf.map_fn(lambda i: raw_labels[i + total_time_steps - lags: i + total_time_steps, :],
                           tf.range(t - total_time_steps + 1),
                           dtype=tf.float32)
        labels = tf.ensure_shape(labels, [None, lags, None])
        print(f'labels = {labels}')
        return tf.data.Dataset.zip((tf.data.Dataset.from_tensor_slices(inputs),
                                    tf.data.Dataset.from_tensor_slices(labels)))

    def process_dataset(dataset):
        def reformat(*args):
            print(f'reformat args[0].shape = {args[0].shape}, args[1].shape = {args[1].shape}', flush=True)
            return {"inputs": args[0]}, args[1]

        def filter_by_length(raw_tensors_dict, raw_labels, total_time_steps):
            raw_inputs = raw_tensors_dict['inputs']
            print(f'raw_inputs = {raw_inputs}')
            print(f'raw_labels = {raw_labels}')
            t = tf.shape(raw_inputs)[0]
            return t >= total_time_steps

        def filter_example_by_length(raw_tensors_dict, raw_labels):
            return filter_by_length(raw_tensors_dict, raw_labels, int(fixed_params['total_time_steps']))

        return dataset.filter(filter_example_by_length).flat_map(process_example).map(reformat)

    def process_example(raw_tensors_dict, raw_labels):
        return expand_raw_time_series(raw_tensors_dict, raw_labels,
                                      int(fixed_params['total_time_steps']),
                                      int(fixed_params['total_time_steps']) - int(fixed_params['num_encoder_steps']))

    # train config
    only_one_worker = True
    raw_dataset_fn = lambda: tf.data.TFRecordDataset(task_config.dataset_file)
    valid_raw_dataset_fn = None
    train_dataset_length = task_config.dataset_length

    if only_one_worker:
        os.environ.pop('TF_CONFIG', None)
        local_valid_ratio = float(user_params.get('validation_split', 0.1))
        valid_dataset_length = max(int(task_config.dataset_length * local_valid_ratio), 1)
        logging.info("valid_dataset_length: {}".format(valid_dataset_length))
        raw_dataset_fn = lambda: tf.data.TFRecordDataset(task_config.dataset_file).skip(valid_dataset_length)
        valid_raw_dataset_fn = lambda: tf.data.TFRecordDataset(task_config.dataset_file).take(valid_dataset_length)
        train_dataset_length = train_dataset_length - valid_dataset_length
        logging.info(f'train_dataset_length = {train_dataset_length}, valid_dataset_length = {valid_dataset_length}')

    train_config = {
        'filenames': task_config.dataset_file,
        'raw_dataset_fn': raw_dataset_fn,
        'valid_raw_dataset_fn': valid_raw_dataset_fn,
        'post_process_dataset': process_dataset,
        'batch_size': int(user_params['batch_size']),
        'num_epochs': int(user_params['num_epochs']),
        'log_step_count_steps': 1,
        'save_checkpoints_steps': int(user_params['save_checkpoints_steps']),
        'early_stopping_patience_steps': int(user_params['early_stopping_patience_steps']),
        'gpu_devices': gpu_devices,
    }

    # export config
    save_best_only = user_params.get('save_best_only') == 'true'
    best_exporter_metric = user_params.get('best_exporter_metric', 'loss')
    metric_bigger = best_exporter_metric in [
        'acc', 'auc', 'binary_accuracy', 'precision', 'recall', 'true_negatives', 'true_positives',
        'sparse_categorical_accuracy'
    ]
    export_config = {
        'placeholders_config': [{
            'name': tensor_col,
            'dtype': 'float',
            'shape': [None, *tensor_shapes[tensor_col][1:]],
        }],
        'export_batch_dim': True,
        'exporter_type': 'best' if save_best_only else 'latest',
        'best_exporter_metric': best_exporter_metric,
        'metric_bigger': metric_bigger,
    }
    logging.info("export_config = {}".format(export_config))

    model_params = json.loads(user_params['model_params'])
    print(f'model_params = {model_params}', flush=True)
    column_definitions = json.loads(user_params['column_definitions'])
    print(f'raw column_definitions = {column_definitions}', flush=True)
    column_definitions = list(map(lambda d: (d[0], DataTypes[d[1]], InputTypes[d[2]]), column_definitions))
    print(f'column_definitions = {column_definitions}', flush=True)
    model_dir = user_params.get("ALINK:checkpoint_path")
    print(f'model_dir = {model_dir}', flush=True)

    formatter = AlinkDataFormatter(column_definitions, fixed_params, model_params)
    model_folder = os.path.join(task_config.work_dir, "model_folder")

    params = {
        **formatter.get_experiment_params(),
        **model_params,
        'model_folder': model_folder
    }

    if 'category_counts' not in params or params['category_counts'] is None:
        category_counts_path = os.path.join(task_config.work_dir, "bc_data_1")
        if not os.path.exists(category_counts_path):
            raise FileNotFoundError(f'File {category_counts_path} not found.')
        with open(category_counts_path, 'r') as f:
            category_counts = json.load(f)
        print(f'category_counts = {category_counts}', flush=True)
        params.update({
            'category_counts': category_counts
        })

    print(f'model params = {params}', flush=True)
    model = TemporalFusionTransformer(params, use_cudnn=False)
    estimator = tf.keras.estimator.model_to_estimator(model.model,
                                                      model_dir=model_dir,
                                                      config=generate_run_config(**train_config))
    sys.stdout.flush()
    if not only_one_worker:
        train_estimator(estimator, input_config, train_config, export_config, task_config)
    else:
        train_estimator_one_worker(estimator, input_config, train_config, export_config, task_config)
