import json

from akdl.models.tf.keras_sequential import keras_sequential_main
from akdl.runner.config import TrainTaskConfig

import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

# tf.compat.v1.enable_eager_execution()


def test_keras_sequential_multi_classification(tmp_path):
    print(tmp_path)
    tensor_shapes = {
        'tensor': [784],
    }
    json.dump(tensor_shapes, open(tmp_path / 'tensor_shapes.txt', "w"))

    with open(tmp_path / 'bc_data_1', 'w') as f:
        f.write("10")

    model_config = {
        'layers': [
            "Reshape((28, 28, 1))",
            "Conv2D(5, 7)",
            "Flatten()",
            "Dense(1024, activation='relu')",
            "Dense(512, activation='relu')",
        ]
    }

    user_params = {
        'tensor_cols': json.dumps(['tensor']),
        'label_col': 'label',
        'label_type': 'double',
        'batch_size': '32',
        'num_epochs': '2',
        'model_config': json.dumps(model_config),
        'bc_1': str(tmp_path / 'bc_data_1')
    }
    args: TrainTaskConfig = TrainTaskConfig(
        dataset_file='multi_classification_dataset.tfrecords',
        tf_context=None,
        num_workers=1, cluster=None, task_type='chief', task_index=0,
        work_dir=str(tmp_path),
        dataset=None, dataset_length=100,
        saved_model_dir=str(tmp_path / 'saved_model_dir'),
        user_params=user_params)
    keras_sequential_main.main(args)


def test_keras_sequential_binary_classification(tmp_path):
    print(tmp_path)
    tensor_shapes = {
        'tensor': [784],
    }
    json.dump(tensor_shapes, open(tmp_path / 'tensor_shapes.txt', "w"))

    with open(tmp_path / 'bc_data_1', 'w') as f:
        f.write("2")

    model_config = {
        'layers': [
            "Dense(1024, activation='relu')",
            "Dense(512, activation='relu')",
        ]
    }

    user_params = {
        'tensor_cols': json.dumps(['tensor']),
        'label_col': 'label',
        'label_type': 'float',
        'batch_size': '32',
        'num_epochs': '2',
        'model_config': json.dumps(model_config),
        'bc_1': str(tmp_path / 'bc_data_1')
    }
    args: TrainTaskConfig = TrainTaskConfig(
        dataset_file='binary_classification_dataset.tfrecords',
        tf_context=None,
        num_workers=1, cluster=None, task_type='chief', task_index=0,
        work_dir=str(tmp_path),
        dataset=None, dataset_length=100,
        saved_model_dir=str(tmp_path / 'saved_model_dir'),
        user_params=user_params)
    keras_sequential_main.main(args)


def test_keras_sequential_regression(tmp_path):
    print(tmp_path)
    tensor_shapes = {
        'tensor': [784],
    }
    json.dump(tensor_shapes, open(tmp_path / 'tensor_shapes.txt', "w"))

    model_config = {
        'layers': [
            "Dense(1024, activation='relu')",
            "Dense(512, activation='relu')",
        ]
    }

    user_params = {
        'tensor_cols': json.dumps(['tensor']),
        'label_col': 'label',
        'label_type': 'float',
        'batch_size': '32',
        'num_epochs': '10',
        'model_config': json.dumps(model_config),
        'optimizer': 'Adam(learning_rate=0.1)'
    }
    args: TrainTaskConfig = TrainTaskConfig(
        dataset_file='regression_dataset.tfrecords',
        tf_context=None,
        num_workers=1, cluster=None, task_type='chief', task_index=0,
        work_dir=str(tmp_path),
        dataset=None, dataset_length=100,
        saved_model_dir=str(tmp_path / 'saved_model_dir'),
        user_params=user_params)
    keras_sequential_main.main(args)
