from abc import ABC

from ..batch.lazy_evaluation import PipeLazyEvaluationConsumer, LazyEvaluation, to_j_consumer_list
from ..common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ..common.types.conversion.type_converters import j_value_to_py_value
from ..common.utils.printing import print_with_title
from ..py4j_util import get_java_class

__all__ = ['HasLazyPrintModelInfo', 'HasLazyPrintTrainInfo', 'HasLazyPrintTransformInfo']


class HasLazyPrintModelInfo(JavaObjectWrapper, ABC):
    def enableLazyPrintModelInfo(self, title: str = None):
        """
        After enabled, if :py:func:`fit` is called, model information will be printed after the program execution triggered.

        :param title: title prepended to model information.
        :return: `self`.
        """
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


class HasLazyPrintTrainInfo(JavaObjectWrapper, ABC):
    def enableLazyPrintTrainInfo(self, title: str = None):
        """
        After enabled, if :py:func:`fit` is called, training information will be printed after the program execution triggered.

        :param title: title prepended to training information.
        :return: `self`.
        """
        self.get_j_obj().enableLazyPrintTrainInfo(title)

        j_pipeline_lazy_printer_cls = get_java_class("com.alibaba.alink.common.lazy.PipelineLazyCallbackUtils")

        j_lazy_train_info = LazyEvaluation()
        py_lazy_train_info = j_lazy_train_info.transform(j_value_to_py_value)
        py_lazy_train_info.addCallback(lambda d: print_with_title(d, title))

        j_consumer = PipeLazyEvaluationConsumer(j_lazy_train_info)

        j_array_list_cls = get_java_class("java.util.ArrayList")
        j_consumer_list = j_array_list_cls()
        j_consumer_list.add(j_consumer)

        j_pipeline_lazy_printer_cls.callbackForTrainerLazyTrainInfo(self.get_j_obj(), j_consumer_list)
        return self


class HasLazyPrintTransformInfo(JavaObjectWrapper, ABC):
    def enableLazyPrintTransformData(self, n: int, title: str = None):
        """
        After enabled, if :py:func:`transform` is called, transformation result will be printed after the program execution triggered.

        :param n: number of items to be printed.
        :param title: title prepended to transformation result.
        :return: `self`.
        """

        def add_callback_to_transform_result(j_transform_result):
            from ..batch.base import BatchOperatorWrapper
            transform_result = BatchOperatorWrapper(j_transform_result)
            transform_result.lazyPrint(n, title)

        self.get_j_obj().enableLazyPrintTransformData(n, title)

        j_pipeline_lazy_printer_cls = get_java_class("com.alibaba.alink.common.lazy.PipelineLazyCallbackUtils")

        j_lazy_transform_result = LazyEvaluation()
        j_lazy_transform_result.addCallback(
            lambda j_transform_result: add_callback_to_transform_result(j_transform_result)
        )

        j_consumer = PipeLazyEvaluationConsumer(j_lazy_transform_result)
        from .base import Estimator
        if isinstance(self, Estimator):
            j_pipeline_lazy_printer_cls.callbackForTrainerLazyTransformResult(self.get_j_obj(),
                                                                              to_j_consumer_list(j_consumer))
        else:
            j_pipeline_lazy_printer_cls.callbackForTransformerLazyTransformResult(self.get_j_obj(),
                                                                                  to_j_consumer_list(j_consumer))
        return self

    def enableLazyPrintTransformStat(self, title: str = None):
        """
        After enabled, if :py:func:`transform` is called, statistics of transformation result will be printed after the program execution triggered.

        :param title: title prepended to statistics.
        :return: `self`.
        """

        def add_callback_to_transform_result(j_transform_result):
            from ..batch.base import BatchOperatorWrapper
            transform_result = BatchOperatorWrapper(j_transform_result)
            transform_result.lazyPrintStatistics(title)

        self.get_j_obj().enableLazyPrintTransformStat(title)

        j_pipeline_lazy_printer_cls = get_java_class("com.alibaba.alink.common.lazy.PipelineLazyCallbackUtils")

        j_lazy_transform_result = LazyEvaluation()
        j_lazy_transform_result.addCallback(
            lambda j_transform_result: add_callback_to_transform_result(j_transform_result)
        )

        j_consumer = PipeLazyEvaluationConsumer(j_lazy_transform_result)
        from .base import Estimator
        if isinstance(self, Estimator):
            j_pipeline_lazy_printer_cls.callbackForTrainerLazyTransformResult(self.get_j_obj(),
                                                                              to_j_consumer_list(j_consumer))
        else:
            j_pipeline_lazy_printer_cls.callbackForTransformerLazyTransformResult(self.get_j_obj(),
                                                                                  to_j_consumer_list(j_consumer))
        return self
