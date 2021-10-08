import json

from akdl.models.tf.deepar import deepar_main
from akdl.runner.config import TrainTaskConfig

import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

# tf.compat.v1.enable_eager_execution()


def test_deepar(tmp_path):
    print(tmp_path)
    tensor_shapes = {
        'tensor': [200, 3],
        'label': [200]
    }
    json.dump(tensor_shapes, open(tmp_path / 'tensor_shapes.txt', "w"))

    model_config = {
        'window': 7 * 23
    }

    user_params = {
        'tensorCol': 'tensor',
        'labelCol': 'label',
        'batch_size': 20,
        'num_epochs': 3,
        'model_config': json.dumps(model_config)
    }
    args: TrainTaskConfig = TrainTaskConfig(
        dataset_file='dataset.tfrecords',
        tf_context=None,
        num_workers=1, cluster=None, task_type='chief', task_index=0,
        work_dir=str(tmp_path),
        dataset=None, dataset_length=100,
        saved_model_dir=str(tmp_path / 'saved_model_dir'),
        user_params=user_params)
    deepar_main.main(args)
