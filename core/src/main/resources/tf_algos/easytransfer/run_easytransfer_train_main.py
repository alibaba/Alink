import os

os.environ["HOME"] = os.path.expanduser('~')

from akdl.models.tf.easytransfer import easytransfer_main
from akdl.runner.config import TrainTaskConfig


def main(task_config: TrainTaskConfig):
    easytransfer_main.main(task_config)
