from ...common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ...common.types.conversion.java_method_call import call_java_method

__all__ = ['ParamGrid']


class ParamGrid(JavaObjectWrapper):
    _j_cls_name = "com.alibaba.alink.pipeline.tuning.ParamGrid"

    def __init__(self, j_param_dist=None):
        if j_param_dist is None:
            self._j_param_dist = ParamGrid._j_cls()()
            self.items = []
        else:
            # Just for wrapping the return value of `self.get_j_obj().addGrid`, which should be ignored.
            self._j_param_dist = j_param_dist

    def get_j_obj(self):
        return self._j_param_dist

    def addGrid(self, stage, info, params):
        """
        Add value grid `params` for parameter `info` in the `stage`.

        :param stage: the stage where the parameter belongs to.
        :param info: name of the parameter, must be snake case with all letters capitalized, e.g. `NUM_TREES`.
        :param params: all value choices for the parameter.
        :return: `self`.
        """
        if len(params) <= 0:
            raise Exception("at least 1 item should be provided in addGrid")
        call_java_method(self.get_j_obj().addGrid, stage, stage.get_j_obj().__getattr__(info), params)
        self.items.append((stage, info, params))
        return self

    def getItems(self):
        """
        Get all parameters and corresponding grids.

        :return: all parameters and corresponding grids.
        """
        return self.items
