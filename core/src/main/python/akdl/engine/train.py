import logging

import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

from .inputs import get_dataset_fn, parse_feature_specs, get_feature_placeholders
from ..runner.config import TrainTaskConfig


def train_estimator(estimator: tf.estimator.Estimator, input_config, train_config, export_config,
                    task_config: TrainTaskConfig):
    example_config = input_config['example_config']
    label_col = input_config['label_col']

    feature_specs = parse_feature_specs(example_config)
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
