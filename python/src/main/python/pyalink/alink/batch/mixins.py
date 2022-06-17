from abc import ABC

from .base import BatchOperatorWrapper
from .lazy_evaluation import pipe_j_lazy_to_py_callbacks
from ..common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ..common.types.conversion.java_method_call import auto_convert_java_type
from ..common.types.conversion.type_converters import j_value_to_py_value
from ..common.utils.printing import print_with_title

__all__ = ['ExtractModelInfoBatchOp', 'WithModelInfoBatchOp', 'WithTrainInfo', 'EvaluationMetricsCollector']


class ExtractModelInfoBatchOp(JavaObjectWrapper, ABC):
    def lazyPrintModelInfo(self, title=None):
        return self.lazyCollectModelInfo(lambda d: print_with_title(d, title))

    @auto_convert_java_type
    def collectModelInfo(self):
        return self.collectModelInfo()

    def lazyCollectModelInfo(self, *callbacks):
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectModelInfo,
            callbacks,
            j_value_to_py_value
        )
        return self


class WithModelInfoBatchOp(JavaObjectWrapper, ABC):
    def getModelInfoBatchOp(self):
        return BatchOperatorWrapper(self.get_j_obj().getModelInfoBatchOp())

    def lazyPrintModelInfo(self, title=None):
        return self.lazyCollectModelInfo(lambda d: print_with_title(d, title))

    @auto_convert_java_type
    def collectModelInfo(self):
        return self.collectModelInfo()

    def lazyCollectModelInfo(self, *callbacks):
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectModelInfo,
            callbacks,
            j_value_to_py_value
        )
        return self


class WithTrainInfo(JavaObjectWrapper, ABC):
    def getTrainInfoBatchOp(self):
        return BatchOperatorWrapper(self.get_j_obj().getTrainInfoBatchOp())

    def lazyPrintTrainInfo(self, title=None):
        return self.lazyCollectTrainInfo(lambda d: print_with_title(d, title))

    @auto_convert_java_type
    def collectTrainInfo(self):
        return self.collectTrainInfo()

    def lazyCollectTrainInfo(self, *callbacks):
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectTrainInfo,
            callbacks,
            j_value_to_py_value
        )
        return self


class EvaluationMetricsCollector(JavaObjectWrapper, ABC):
    """
    Mixin for batch evaluation operators, provides APIs about obtaining metrics.
    """

    @auto_convert_java_type
    def collectMetrics(self):
        """
        Trigger the program execution like :py:func:`BatchOperator.execute`, and
        collect data in the operator to an instance of a subtype of :py:class:`BaseMetrics`.

        :return: an instance of a subtype of :py:class:`BaseMetrics`.
        """
        return self.collectMetrics()

    def lazyCollectMetrics(self, *callbacks):
        """
        Lazily collect data in this operator to an instance of a subtype of :py:class:`BaseMetrics`, and call `callbacks`.

        :param callbacks: callback functions applied to a subtype of :py:class:`BaseMetrics`.
        :return: `self`.
        """
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectMetrics,
            callbacks,
            j_value_to_py_value)
        return self

    def lazyPrintMetrics(self, title: str = None):
        """
        Lazily print data in this operator as an instance of a subtype of :py:class:`BaseMetrics`, e.g. the print action is performed when next execution triggered.

        :param title: title prepended to the data.
        :return: `self`.
        """
        return self.lazyCollectMetrics(lambda metrics: print_with_title(metrics, title))
