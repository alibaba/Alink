from ...common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ...common.types.conversion.java_method_call import call_java_method
from ...py4j_util import get_java_class


class ParamGrid(JavaObjectWrapper):
    def get_j_obj(self):
        return self._j_param_dist

    def __init__(self):
        self._j_param_dist = get_java_class("com.alibaba.alink.pipeline.tuning.ParamGrid")()
        self.items = []
        pass

    def addGrid(self, stage, info, params):
        if len(params) <= 0:
            raise Exception("at least 1 item should be provided in addGrid")
        call_java_method(self.get_j_obj().addGrid, stage, stage.get_j_obj().__getattr__(info), params)
        self.items.append((stage, info, params))
        return self

    def getItems(self):
        return self.items
