__all__ = ['TrainTaskConfig', 'BatchTaskConfig', 'StreamTaskConfig']


class BaseConfig:
    def __init__(self, tf_context, num_workers, cluster, task_type, task_index, work_dir, user_params, **kwargs):
        self.tf_context = tf_context
        self.num_workers = num_workers
        self.cluster = cluster
        self.task_type = task_type
        self.task_index = task_index
        self.work_dir = work_dir
        self.user_params = user_params


class TrainTaskConfig(BaseConfig):
    def __init__(self, dataset_file, dataset_length, saved_model_dir, **kwargs):
        super(TrainTaskConfig, self).__init__(**kwargs)
        self.dataset_file = dataset_file
        self.dataset_length = dataset_length
        self.saved_model_dir = saved_model_dir


class BatchTaskConfig(BaseConfig):
    def __init__(self, dataset_file, dataset_length, output_writer, **kwargs):
        super(BatchTaskConfig, self).__init__(**kwargs)
        self.dataset_file = dataset_file
        self.dataset_length = dataset_length
        self.output_writer = output_writer


class StreamTaskConfig(BaseConfig):
    def __init__(self, dataset_fn, output_writer, **kwargs):
        super(StreamTaskConfig, self).__init__(**kwargs)
        self.dataset_fn = dataset_fn
        self.output_writer = output_writer
