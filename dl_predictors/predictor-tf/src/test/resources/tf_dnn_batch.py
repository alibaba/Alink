from akdl.models.tf.dnn import dnn_batch
from akdl.runner.config import BatchTaskConfig


def main(task_config: BatchTaskConfig):
    dnn_batch.main(task_config)
