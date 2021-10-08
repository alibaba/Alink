import json

from akdl.models.tf.dnn.dnn_tensor import dnn_tensor
from akdl.runner.config import TrainTaskConfig


def test_dnn_tensor(tmp_path):
    print(tmp_path)
    tensor_shapes = {
        't': [10, 15]
    }
    json.dump(tensor_shapes, open(tmp_path / 'tensor_shapes.txt', "w"))

    user_params = {
        'tensorCol': 't',
        'labelCol': 'label',
        'batch_size': 20,
        'num_epochs': 100,
    }
    args: TrainTaskConfig = TrainTaskConfig(dataset_file='lstnet/dataset.tfrecords', tf_context=None,
                                            num_workers=1, cluster=None, task_type='chief', task_index=0,
                                            work_dir=str(tmp_path),
                                            dataset=None, dataset_length=100,
                                            saved_model_dir=str(tmp_path / 'saved_model_dir'),
                                            user_params=user_params)
    dnn_tensor(args)
