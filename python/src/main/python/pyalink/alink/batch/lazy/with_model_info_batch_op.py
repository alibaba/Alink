from abc import ABC

from .lazy_evaluation import pipe_j_lazy_to_py_callbacks
from ..base import BatchOperatorWrapper
from ...common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ...common.types.conversion.java_method_call import auto_convert_java_type
from ...common.types.conversion.type_converters import j_value_to_py_value
from ...common.utils.printing import print_with_title


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
