from abc import ABC

from ...batch.lazy.lazy_evaluation import PipeLazyEvaluationConsumer, LazyEvaluation
from ...common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ...common.types.conversion.type_converters import j_value_to_py_value
from ...common.utils.printing import print_with_title
from ...py4j_util import get_java_class


class HasLazyPrintModelInfo(JavaObjectWrapper, ABC):
    def enableLazyPrintModelInfo(self, title=None):
        self.get_j_obj().enableLazyPrintModelInfo(title)

        j_pipeline_lazy_printer_cls = get_java_class("com.alibaba.alink.common.lazy.PipelineLazyCallbackUtils")

        j_lazy_train_info = LazyEvaluation()
        py_lazy_train_info = j_lazy_train_info.transform(j_value_to_py_value)
        py_lazy_train_info.addCallback(lambda d: print_with_title(d, title))

        j_consumer = PipeLazyEvaluationConsumer(j_lazy_train_info)

        j_array_list_cls = get_java_class("java.util.ArrayList")
        j_consumer_list = j_array_list_cls()
        j_consumer_list.add(j_consumer)

        j_pipeline_lazy_printer_cls.callbackForTrainerLazyModelInfo(self.get_j_obj(), j_consumer_list)
        return self
