from .common import ChiSquareTestBatchOp as _ChiSquareTestBatchOp
from .common import CorrelationBatchOp as _CorrelationBatchOp
from .common import SummarizerBatchOp as _SummarizerBatchOp
from .common import VectorChiSquareTestBatchOp as _VectorChiSquareTestBatchOp
from .common import VectorCorrelationBatchOp as _VectorCorrelationBatchOp
from .common import VectorSummarizerBatchOp as _VectorSummarizerBatchOp
from .lazy_evaluation import pipe_j_lazy_to_py_callbacks
from ..common.types.conversion.java_method_call import auto_convert_java_type
from ..common.types.conversion.type_converters import j_value_to_py_value
from ..common.utils.printing import print_with_title

__all__ = ['ChiSquareTestBatchOp', 'CorrelationBatchOp', 'SummarizerBatchOp',
           'VectorChiSquareTestBatchOp', 'VectorCorrelationBatchOp', 'VectorSummarizerBatchOp']


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
