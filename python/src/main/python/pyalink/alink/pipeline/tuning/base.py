from ..base import Estimator
from ..lazy.has_lazy_print_train_info import HasLazyPrintTrainInfo
from ...batch.lazy.lazy_evaluation import PipeLazyEvaluationConsumer, LazyEvaluation
from ...common.types.conversion.type_converters import j_value_to_py_value
from ...common.utils.printing import print_with_title
from ...py4j_util import get_java_class


class BaseTuning(Estimator, HasLazyPrintTrainInfo):
    def get_j_obj(self):
        return self.j_tuning

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls_name = kwargs.pop('CLS_NAME', None)
        self.j_tuning = get_java_class(cls_name)()

    def setEstimator(self, estimator):
        self.get_j_obj().setEstimator(estimator.get_j_obj())
        return self

    def setTuningEvaluator(self, tuning_evaluator):
        self.get_j_obj().setTuningEvaluator(tuning_evaluator.get_j_obj())
        return self

    def enableLazyPrintTrainInfo(self, title=None):
        self.get_j_obj().enableLazyPrintTrainInfo(title)

        j_pipeline_lazy_printer_cls = get_java_class("com.alibaba.alink.common.lazy.PipelineLazyCallbackUtils")

        j_lazy_train_info = LazyEvaluation()
        py_lazy_train_info = j_lazy_train_info.transform(j_value_to_py_value)
        py_lazy_train_info.addCallback(lambda d: print_with_title(d, title))

        j_consumer = PipeLazyEvaluationConsumer(j_lazy_train_info)

        j_array_list_cls = get_java_class("java.util.ArrayList")
        j_consumer_list = j_array_list_cls()
        j_consumer_list.add(j_consumer)

        j_pipeline_lazy_printer_cls.callbackForTuningLazyReport(self.get_j_obj(), j_consumer_list)
        return self


class BaseGridSearch(BaseTuning):
    def __init__(self, *args, **kwargs):
        super(BaseGridSearch, self).__init__(*args, **kwargs)

    def setParamGrid(self, value):
        self.get_j_obj().setParamGrid(value.get_j_obj())
        return self


class BaseRandomSearch(BaseTuning):
    def __init__(self, *args, **kwargs):
        super(BaseRandomSearch, self).__init__(*args, **kwargs)

    def setParamDist(self, value):
        self.get_j_obj().setParamDist(value.get_j_obj())
        return self
