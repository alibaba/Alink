import json
import logging

from akdl.engine.run_config import generate_run_config
from akdl.engine.train import train_estimator
from akdl.runner.config import TrainTaskConfig

import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1


def main(task_config: TrainTaskConfig):
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
        'raw_dataset_fn': lambda: tf.data.TFRecordDataset(task_config.dataset_file),
        'batch_size': int(user_params['batch_size']),
        'num_epochs': int(user_params['num_epochs']),
        'log_step_count_steps': 1
    }

    export_config = {
        'placeholders_config': [
            {
                'name': name,
                'dtype': 'float',
                'shape': [1]
            }
            for name in feature_cols
        ]
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

    train_estimator(classifier, input_config, train_config, export_config, task_config)
