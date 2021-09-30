import json
import logging
import os

from akdl.engine.inputs import get_feature_placeholders, parse_feature_specs, get_dataset_fn
from akdl.engine.run_config import generate_run_config
from akdl.runner import io_helper
from akdl.runner.config import StreamTaskConfig

import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1


def main(task_config: StreamTaskConfig):
    user_params = task_config.user_params
    feature_cols = json.loads(user_params['featureCols'])
    label_col = user_params['labelCol']

    example_config = [
        {
            'name': name,
            'dtype': 'float',
            'shape': [1]
        }
        for name in feature_cols
    ]
    example_config.append({
        'name': label_col,
        'dtype': 'long',
        'shape': [1]
    })
    print(example_config)

    input_config = {
        'example_config': example_config,
        'label_col': label_col
    }

    train_config = {
        # 'filenames': task_config.dataset_file,
        'batch_size': int(user_params['batch_size']),
        'num_epochs': int(user_params['num_epochs']),
        'log_step_count_steps': 1
    }

    logging.info("feature_cols: {}".format(feature_cols))
    logging.info("feature_cols!: {}".format(feature_cols))
    feature_columns = [
        tf.feature_column.numeric_column(name)
        for name in feature_cols
    ]
    logging.info("label_col: {}".format(label_col))

    classifier = tf.estimator.DNNClassifier(
        feature_columns=feature_columns,
        hidden_units=[30, 10],
        n_classes=2,
        config=generate_run_config(**train_config))
    estimator = classifier

    example_config = input_config['example_config']
    label_col = input_config['label_col']

    feature_specs = parse_feature_specs(example_config)
    dataset_fn = get_dataset_fn(
        feature_specs=feature_specs,
        raw_dataset_fn=task_config.dataset_fn,
        label_col=label_col,
        **train_config
    )
    train_spec = tf.estimator.TrainSpec(dataset_fn)
    eval_spec = tf.estimator.EvalSpec(dataset_fn)
    tf.estimator.train_and_evaluate(estimator, train_spec, eval_spec)

    feature_placeholders = get_feature_placeholders(example_config, label_col)
    serving_input_receiver_fn = tf.estimator.export.build_raw_serving_input_receiver_fn(feature_placeholders)

    if (task_config.task_type == 'chief' and task_config.task_index == 0) or \
            (task_config.num_workers == 1):
        saved_model_dir = os.path.join(task_config.work_dir, 'saved_model_dir')
        output_writer = task_config.output_writer
        logging.info("Start exporting...")
        estimator.export_saved_model(saved_model_dir,
                                     serving_input_receiver_fn=serving_input_receiver_fn)
        io_helper.output_model_to_flink(saved_model_dir, output_writer)
        logging.info("Finish exporting.")
