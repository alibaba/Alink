import logging
import os.path

import tensorflow as tf
from akdl.runner import io_helper

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

from .inputs import get_dataset_fn, parse_feature_specs, get_feature_placeholders
from ..runner.config import TrainTaskConfig
from .early_stopping import stop_if_no_increase_hook, stop_if_no_decrease_hook


def train_estimator(estimator: tf.estimator.Estimator, input_config, train_config, export_config,
                    task_config: TrainTaskConfig):
    example_config = input_config['example_config']
    label_col = input_config['label_col']

    feature_specs = parse_feature_specs(example_config)
    print(f'feature_specs = {feature_specs}')
    dataset_fn = get_dataset_fn(
        feature_specs=feature_specs,
        label_col=label_col,
        **train_config
    )
    train_spec = tf.estimator.TrainSpec(dataset_fn)
    eval_spec = tf.estimator.EvalSpec(dataset_fn, steps=1)
    tf.estimator.train_and_evaluate(estimator, train_spec, eval_spec)

    feature_placeholders = get_feature_placeholders(**export_config)
    serving_input_receiver_fn = tf.estimator.export.build_raw_serving_input_receiver_fn(feature_placeholders)

    if (task_config.task_type == 'chief' and task_config.task_index == 0) or \
            (task_config.num_workers == 1):
        logging.info("Start exporting...")
        estimator.export_saved_model(task_config.saved_model_dir,
                                     serving_input_receiver_fn=serving_input_receiver_fn)
        logging.info("Finish exporting.")


def train_estimator_one_worker(estimator: tf.estimator.Estimator, input_config, train_config, export_config,
                    task_config: TrainTaskConfig):
    example_config = input_config['example_config']
    label_col = input_config['label_col']

    feature_specs = parse_feature_specs(example_config)
    dataset_fn = get_dataset_fn(
        feature_specs=feature_specs,
        label_col=label_col,
        **train_config
    )

    hooks = []
    if 'early_stopping_patience_steps' in train_config and 'save_checkpoints_steps' in train_config \
            and 'metric_bigger' in export_config and 'best_exporter_metric' in export_config:
        early_stopping_patience = train_config['early_stopping_patience_steps']
        save_checkpoints_steps = train_config['save_checkpoints_steps']
        metric_bigger = export_config['metric_bigger']
        best_exporter_metric = export_config['best_exporter_metric']
        if metric_bigger:
            stop_hook = stop_if_no_increase_hook
        else:
            stop_hook = stop_if_no_decrease_hook
        # NOTE: when running in distribution mode, `run_every_secs` is not supported
        hooks.append(stop_hook(estimator, best_exporter_metric, early_stopping_patience,
                               run_every_secs=None, run_every_steps=save_checkpoints_steps))
    train_spec = tf.estimator.TrainSpec(dataset_fn, hooks=hooks)

    if 'valid_raw_dataset_fn' in train_config:
        kwargs: dict = train_config.copy()
        kwargs.pop('raw_dataset_fn', None)
        kwargs.pop('shuffle_factor', None)
        kwargs.pop('num_epochs', None)
        valid_dataset_fn = get_dataset_fn(
            feature_specs=feature_specs,
            label_col=label_col,
            raw_dataset_fn=train_config['valid_raw_dataset_fn'],
            shuffle_factor=0,
            num_epochs=1,
            **kwargs
        )
    else:
        valid_dataset_fn = lambda: dataset_fn().take(0)

    feature_placeholders = get_feature_placeholders(**export_config)
    serving_input_receiver_fn = tf.estimator.export.build_raw_serving_input_receiver_fn(feature_placeholders)

    exporter_type = export_config.get('exporter_type', 'latest')
    logging.info("exporter_type = {}".format(exporter_type))
    model_dir = estimator.model_dir
    if exporter_type == 'latest':
        exporter = tf.estimator.LatestExporter('latest', serving_input_receiver_fn, exports_to_keep=1)
        export_dir = os.path.join(model_dir, 'export', 'latest')
    elif exporter_type == 'best':
        def _metric_cmp_fn(best_eval_result, current_eval_result):
            logging.info('metric: best = %s current = %s' % (str(best_eval_result), str(current_eval_result)))
            metric_bigger = export_config['metric_bigger']
            best_exporter_metric = export_config['best_exporter_metric']
            if metric_bigger:
                return best_eval_result[best_exporter_metric] < current_eval_result[best_exporter_metric]
            else:
                return best_eval_result[best_exporter_metric] > current_eval_result[best_exporter_metric]

        exporter = tf.estimator.BestExporter(serving_input_receiver_fn=serving_input_receiver_fn,
                                             compare_fn=_metric_cmp_fn,
                                             exports_to_keep=1)
        export_dir = os.path.join(model_dir, 'export', 'best_exporter')
    else:
        raise ValueError("Unsupported exporter_type " + exporter_type)

    eval_spec = tf.estimator.EvalSpec(valid_dataset_fn, steps=None, throttle_secs=0, start_delay_secs=0,
                                      exporters=[exporter])
    tf.estimator.train_and_evaluate(estimator, train_spec, eval_spec)

    if (task_config.task_type == 'chief' and task_config.task_index == 0) or \
            (task_config.num_workers == 1):
        logging.info("Start copying savedmodel.")
        io_helper.gfile_copy_dir_content(export_dir, task_config.saved_model_dir)
        logging.info("Finish copying savedmodel.")


def train_keras_model(model: tf.keras.Model, input_config, train_config, task_config: TrainTaskConfig):
    example_config = input_config['example_config']
    label_col = input_config['label_col']

    feature_specs = parse_feature_specs(example_config)
    dataset_fn = get_dataset_fn(
        feature_specs=feature_specs,
        label_col=label_col,
        **train_config
    )

    model.fit(dataset_fn())

    # train_spec = tf.estimator.TrainSpec(dataset_fn)
    # eval_spec = tf.estimator.EvalSpec(dataset_fn)
    # tf.estimator.train_and_evaluate(estimator, train_spec, eval_spec)

    # feature_placeholders = get_feature_placeholders(example_config, label_col)
    # serving_input_receiver_fn = tf.estimator.export.build_raw_serving_input_receiver_fn(feature_placeholders)
    #
    # if (task_config.task_type == 'chief' and task_config.task_index == 0) or \
    #         (task_config.num_workers == 1):
    #     logging.info("Start exporting...")
    #     estimator.export_saved_model(task_config.saved_model_dir,
    #                                  serving_input_receiver_fn=serving_input_receiver_fn)
    #     logging.info("Finish exporting.")
