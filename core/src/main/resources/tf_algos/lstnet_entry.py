import tensorflow as tf
from akdl.models.tf.lstnet import lstnet_main
from akdl.runner.config import TrainTaskConfig

if tf.__version__ >= '2.0':
    tf = tf.compat.v1


def main(task_config: TrainTaskConfig):
    lstnet_main.main(task_config)
