from .base_entry import BaseEntry
from ..runner import io_helper
from ..runner.config import TrainTaskConfig


class TrainEntry(BaseEntry):
    def construct_args(self, **kwargs):
        return TrainTaskConfig(**kwargs)

    def post_process(self, task_type, task_index, output_writer, saved_model_dir, latest_ckpt_dir=None, **kwargs):
        if task_type == 'chief' and task_index == 0:
            if latest_ckpt_dir is None:
                print("before output_model_to_flink".format(task_type, task_index), flush=True)
                io_helper.output_model_to_flink(saved_model_dir, output_writer)
                print("after output_model_to_flink".format(task_type, task_index), flush=True)
            else:
                if latest_ckpt_dir is not None:
                    print("before output_model_ckpt_to_flink".format(task_type, task_index), flush=True)
                    io_helper.output_model_ckpt_to_flink(saved_model_dir, latest_ckpt_dir, output_writer)
                    print("after output_model_ckpt_to_flink".format(task_type, task_index), flush=True)
