from akdl.entry.base_entry import TF2_TYPE
from akdl.entry.train_entry import TrainEntry
from flink_ml_framework.context import Context


def entry_func(context: Context):
    func_name = "tf_user_main.main"
    entry = TrainEntry(func_name, TF2_TYPE)
    entry.entry_func(context)
