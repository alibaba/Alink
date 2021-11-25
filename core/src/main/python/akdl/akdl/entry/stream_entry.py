from .base_entry import BaseEntry
from ..runner.config import StreamTaskConfig


class StreamEntry(BaseEntry):
    def construct_args(self, **kwargs):
        return StreamTaskConfig(**kwargs)

    def is_batch(self):
        return False
