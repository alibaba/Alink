from .base_entry import BaseEntry
from ..runner import io_helper
from ..runner.config import TrainTaskConfig


class TrainEntry(BaseEntry):
    def construct_args(self, **kwargs):
        return TrainTaskConfig(**kwargs)

    def post_process(self, task_type, task_index, output_writer, saved_model_dir, **kwargs):
        if task_type == 'chief' and task_index == 0:
            print("task_type = {}, task_index = {}: before output_model_to_flink".format(task_type, task_index),
                  flush=True)
            io_helper.output_model_to_flink(saved_model_dir, output_writer)
            print("task_type = {}, task_index = {}: after output_model_to_flink".format(task_type, task_index),
                  flush=True)
