from abc import ABC

from ...batch.lazy.lazy_evaluation import PipeLazyEvaluationConsumer, LazyEvaluation, to_j_consumer_list
from ...common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ...py4j_util import get_java_class


class HasLazyPrintTransformInfo(JavaObjectWrapper, ABC):
    def enableLazyPrintTransformData(self, n, title=None):
        def add_callback_to_transform_result(j_transform_result):
            from ...batch.base import BatchOperatorWrapper
            transform_result = BatchOperatorWrapper(j_transform_result)
            transform_result.lazyPrint(n, title)

        self.get_j_obj().enableLazyPrintTransformData(n, title)

        j_pipeline_lazy_printer_cls = get_java_class("com.alibaba.alink.common.lazy.PipelineLazyCallbackUtils")

        j_lazy_transform_result = LazyEvaluation()
        j_lazy_transform_result.addCallback(
            lambda j_transform_result: add_callback_to_transform_result(j_transform_result)
        )

        j_consumer = PipeLazyEvaluationConsumer(j_lazy_transform_result)
        from ..base import Estimator
        if isinstance(self, Estimator):
            j_pipeline_lazy_printer_cls.callbackForTrainerLazyTransformResult(self.get_j_obj(),
                                                                              to_j_consumer_list(j_consumer))
        else:
            j_pipeline_lazy_printer_cls.callbackForTransformerLazyTransformResult(self.get_j_obj(),
                                                                                  to_j_consumer_list(j_consumer))
        return self

    def enableLazyPrintTransformStat(self, title=None):
        def add_callback_to_transform_result(j_transform_result):
            from ...batch.base import BatchOperatorWrapper
            transform_result = BatchOperatorWrapper(j_transform_result)
            transform_result.lazyPrintStatistics(title)

        self.get_j_obj().enableLazyPrintTransformStat(title)

        j_pipeline_lazy_printer_cls = get_java_class("com.alibaba.alink.common.lazy.PipelineLazyCallbackUtils")

        j_lazy_transform_result = LazyEvaluation()
        j_lazy_transform_result.addCallback(
            lambda j_transform_result: add_callback_to_transform_result(j_transform_result)
        )

        j_consumer = PipeLazyEvaluationConsumer(j_lazy_transform_result)
        from ..base import Estimator
        if isinstance(self, Estimator):
            j_pipeline_lazy_printer_cls.callbackForTrainerLazyTransformResult(self.get_j_obj(),
                                                                              to_j_consumer_list(j_consumer))
        else:
            j_pipeline_lazy_printer_cls.callbackForTransformerLazyTransformResult(self.get_j_obj(),
                                                                                  to_j_consumer_list(j_consumer))
        return self
