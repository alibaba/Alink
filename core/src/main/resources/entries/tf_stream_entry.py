from akdl.entry.base_entry import TF1_TYPE
from akdl.entry.stream_entry import StreamEntry
from flink_ml_framework.context import Context


def entry_func(context: Context):
    func_name = "tf_user_main.main"
    entry = StreamEntry(func_name, TF1_TYPE)
    entry.entry_func(context)
