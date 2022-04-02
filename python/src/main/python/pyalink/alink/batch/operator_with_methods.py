from abc import ABC

from .common import ChiSquareTestBatchOp as _ChiSquareTestBatchOp
from .common import CorrelationBatchOp as _CorrelationBatchOp
from .common import EvalBinaryClassBatchOp as _EvalBinaryClassBatchOp
from .common import EvalClusterBatchOp as _EvalClusterBatchOp
from .common import EvalMultiClassBatchOp as _EvalMultiClassBatchOp
from .common import EvalMultiLabelBatchOp as _EvalMultiLabelBatchOp
from .common import EvalOutlierBatchOp as _EvalOutlierBatchOp
from .common import EvalRankingBatchOp as _EvalRankingBatchOp
from .common import EvalRegressionBatchOp as _EvalRegressionBatchOp
from .common import EvalTimeSeriesBatchOp as _EvalTimeSeriesBatchOp
from .common import SummarizerBatchOp as _SummarizerBatchOp
from .common import VectorChiSquareTestBatchOp as _VectorChiSquareTestBatchOp
from .common import VectorCorrelationBatchOp as _VectorCorrelationBatchOp
from .common import VectorSummarizerBatchOp as _VectorSummarizerBatchOp
from .lazy.lazy_evaluation import pipe_j_lazy_to_py_callbacks
from ..common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ..common.types.conversion.java_method_call import auto_convert_java_type
from ..common.types.conversion.type_converters import j_value_to_py_value
from ..common.utils.printing import print_with_title


class EvaluationMetricsCollector(JavaObjectWrapper, ABC):
    @auto_convert_java_type
    def collectMetrics(self):
        return self.collectMetrics()

    def lazyCollectMetrics(self, *callbacks):
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectMetrics,
            callbacks,
            j_value_to_py_value)
        return self

    def lazyPrintMetrics(self, title: str = None):
        return self.lazyCollectMetrics(lambda metrics: print_with_title(metrics, title))


class EvalBinaryClassBatchOp(_EvalBinaryClassBatchOp, EvaluationMetricsCollector):
    pass


class EvalClusterBatchOp(_EvalClusterBatchOp, EvaluationMetricsCollector):
    pass


class EvalRegressionBatchOp(_EvalRegressionBatchOp, EvaluationMetricsCollector):
    pass


class EvalMultiClassBatchOp(_EvalMultiClassBatchOp, EvaluationMetricsCollector):
    pass


class EvalMultiLabelBatchOp(_EvalMultiLabelBatchOp, EvaluationMetricsCollector):
    pass


class EvalRankingBatchOp(_EvalRankingBatchOp, EvaluationMetricsCollector):
    pass


class EvalTimeSeriesBatchOp(_EvalTimeSeriesBatchOp, EvaluationMetricsCollector):
    pass


class EvalOutlierBatchOp(_EvalOutlierBatchOp, EvaluationMetricsCollector):
    pass


class ChiSquareTestBatchOp(_ChiSquareTestBatchOp):
    @auto_convert_java_type
    def collectChiSquareTest(self):
        return self.collectChiSquareTest()

    def lazyCollectChiSquareTest(self, *callbacks):
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectChiSquareTest,
            callbacks,
            j_value_to_py_value)
        return self

    def lazyPrintChiSquareTest(self, title: str = None):
        return self.lazyCollectChiSquareTest(lambda metrics: print_with_title(metrics, title))


class CorrelationBatchOp(_CorrelationBatchOp):
    @auto_convert_java_type
    def collectCorrelation(self):
        return self.collectCorrelation()

    def lazyCollectCorrelation(self, *callbacks):
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectCorrelation,
            callbacks,
            j_value_to_py_value)
        return self

    def lazyPrintCorrelation(self, title: str = None):
        return self.lazyCollectCorrelation(lambda results: print_with_title(results, title))


class SummarizerBatchOp(_SummarizerBatchOp):
    @auto_convert_java_type
    def collectSummary(self):
        return self.collectSummary()

    def lazyCollectSummary(self, *callbacks):
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectSummary,
            callbacks,
            j_value_to_py_value)
        return self

    def lazyPrintSummary(self, title: str = None):
        return self.lazyCollectSummary(lambda summary: print_with_title(summary, title))


class VectorChiSquareTestBatchOp(_VectorChiSquareTestBatchOp):
    @auto_convert_java_type
    def collectChiSquareTest(self):
        return self.collectChiSquareTest()

    def lazyCollectChiSquareTest(self, *callbacks):
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectChiSquareTest,
            callbacks,
            j_value_to_py_value)
        return self

    def lazyPrintChiSquareTest(self, title: str = None):
        return self.lazyCollectChiSquareTest(lambda results: print_with_title(results, title))


class VectorCorrelationBatchOp(_VectorCorrelationBatchOp):
    @auto_convert_java_type
    def collectCorrelation(self):
        return self.collectCorrelation()

    def lazyCollectCorrelation(self, *callbacks):
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectCorrelation,
            callbacks,
            j_value_to_py_value)
        return self

    def lazyPrintCorrelation(self, title: str = None):
        return self.lazyCollectCorrelation(lambda results: print_with_title(results, title))


class VectorSummarizerBatchOp(_VectorSummarizerBatchOp):
    @auto_convert_java_type
    def collectVectorSummary(self):
        return self.collectVectorSummary()

    def lazyCollectVectorSummary(self, *callbacks):
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectVectorSummary,
            callbacks,
            j_value_to_py_value)
        return self

    def lazyPrintVectorSummary(self, title: str = None):
        return self.lazyCollectVectorSummary(lambda summary: print_with_title(summary, title))
