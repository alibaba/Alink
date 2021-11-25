from .base_entry import BaseEntry
from ..runner.config import BatchTaskConfig


class BatchEntry(BaseEntry):
    def construct_args(self, **kwargs):
        return BatchTaskConfig(**kwargs)
