from ...common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ...common.types.conversion.java_method_call import call_java_method
from ...py4j_util import get_java_class


class ValueDist(JavaObjectWrapper):

    def __init__(self, j_value_dist):
        self._j_value_dist = j_value_dist

    def get_j_obj(self):
        return self._j_value_dist

    @staticmethod
    def get_j_value_dist_cls():
        return get_java_class("com.alibaba.alink.pipeline.tuning.ValueDist")

    @staticmethod
    def randInteger(start, end):
        return ValueDist(ValueDist.get_j_value_dist_cls().randInteger(start, end))

    @staticmethod
    def randLong(start, end):
        return ValueDist(ValueDist.get_j_value_dist_cls().randLong(start, end))

    @staticmethod
    def randArray(values):
        if len(values) <= 0:
            raise Exception("at least 1 item should be provided in randArray")
        return ValueDist(call_java_method(ValueDist.get_j_value_dist_cls().randArray, values))

    @staticmethod
    def exponential(e):
        return ValueDist(ValueDist.get_j_value_dist_cls().exponential(e))

    @staticmethod
    def uniform(lowerbound, upperbound):
        return ValueDist(ValueDist.get_j_value_dist_cls().uniform(lowerbound, upperbound))

    @staticmethod
    def normal(mu, sigma2):
        return ValueDist(ValueDist.get_j_value_dist_cls().normal(mu, sigma2))

    @staticmethod
    def stdNormal():
        return ValueDist(ValueDist.get_j_value_dist_cls().stdNormal())

    @staticmethod
    def chi2(df):
        return ValueDist(ValueDist.get_j_value_dist_cls().chi2(df))

    def get(self, p):
        return self.get_j_obj().get(p)


class ParamDist(JavaObjectWrapper):
    def get_j_obj(self):
        return self.j_param_dist

    def __init__(self):
        j_param_dist_cls = get_java_class("com.alibaba.alink.pipeline.tuning.ParamDist")
        self.j_param_dist = j_param_dist_cls()
        self.items = []

    def addDist(self, stage, info, dist):
        self.get_j_obj().addDist(stage.get_j_obj(), stage.get_j_obj().__getattr__(info), dist.get_j_obj())
        self.items.append((stage, info, dist))
        return self

    def getItems(self):
        return self.items
