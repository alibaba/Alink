from typing import Any, List, Union

from py4j.java_gateway import JavaObject

from ..base import Estimator, Transformer
from ...common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ...common.types.conversion.java_method_call import call_java_method

__all__ = ['ValueDist', 'ParamDist']


class ValueDist(JavaObjectWrapper):
    _j_cls_name = "com.alibaba.alink.pipeline.tuning.ValueDist"

    def __init__(self, j_value_dist: JavaObject):
        self._j_value_dist = j_value_dist

    def get_j_obj(self):
        return self._j_value_dist

    @staticmethod
    def randInteger(start: int, end: int) -> 'ValueDist':
        """
        A distribution to uniformly generate integers in [start, end].

        :param start: start of interval.
        :param end: end of interval.
        :return: the distribution described above.
        """
        return ValueDist(ValueDist._j_cls().randInteger(start, end))

    @staticmethod
    def randLong(start: int, end: int) -> 'ValueDist':
        """
        A distribution to uniformly generate longs in [start, end].

        :param start: start of interval.
        :param end: end of interval.
        :return: the distribution described above.
        """
        return ValueDist(ValueDist._j_cls().randLong(start, end))

    @staticmethod
    def randArray(values: List[Any]) -> 'ValueDist':
        """
        A distribution to uniformly choose a value in `values` once at a time.

        :param values: values to choose from.
        :return: the distribution described above.
        """
        if len(values) <= 0:
            raise Exception("at least 1 item should be provided in randArray")
        return ValueDist(call_java_method(ValueDist._j_cls().randArray, values))

    @staticmethod
    def exponential(e: float) -> 'ValueDist':
        """
        Exponential distribution with parameter `e`.

        :param e: distribution parameter.
        :return: the distribution described above.
        """
        return ValueDist(ValueDist._j_cls().exponential(e))

    @staticmethod
    def uniform(lowerbound: float, upperbound: float) -> 'ValueDist':
        """
        Continuous uniform distribution in interval `[lowerbound, upperbound]`.

        :param lowerbound: lowerbound of interval.
        :param upperbound: upperbound of interval.
        :return: the distribution described above.
        """
        return ValueDist(ValueDist._j_cls().uniform(lowerbound, upperbound))

    @staticmethod
    def normal(mu, sigma2) -> 'ValueDist':
        """
        Normal distribution with parameter `mu` and `sigma`.

        :param mu: mean of the distribution.
        :param sigma2: standard deviation for this distribution.
        :return: the distribution described above.
        """
        return ValueDist(ValueDist._j_cls().normal(mu, sigma2))

    @staticmethod
    def stdNormal() -> 'ValueDist':
        """
        Standard normal distribution.

        :return: the distribution described above.
        """
        return ValueDist(ValueDist._j_cls().stdNormal())

    @staticmethod
    def chi2(df):
        """
        Chi squared distribution with parameter `df`.

        :param df: degrees of freedom.
        :return: the distribution described above.
        """
        return ValueDist(ValueDist._j_cls().chi2(df))

    def get(self, p: float):
        """
        Get corresponding value for accumulative probability `p`.

        :param p: the accumulative probability, in range [0, 1].
        :return: corresponding value.
        """
        return self.get_j_obj().get(p)


class ParamDist(JavaObjectWrapper):
    _j_cls_name = "com.alibaba.alink.pipeline.tuning.ParamDist"

    def __init__(self):
        self.j_param_dist = ParamDist._j_cls()()
        self.items = []

    def get_j_obj(self):
        return self.j_param_dist

    def addDist(self, stage: Union[Estimator, Transformer], info: str, dist: ValueDist) -> 'ParamDist':
        """
        Add value distribution `dist` for parameter `info` in the `stage`.

        :param stage: the stage where the parameter belongs to.
        :param info: name of the parameter, must be snake case with all letters capitalized, e.g. `NUM_TREES`.
        :param dist: value distribution for the parameter.
        :return: `self`.
        """
        self.get_j_obj().addDist(stage.get_j_obj(), stage.get_j_obj().__getattr__(info), dist.get_j_obj())
        self.items.append((stage, info, dist))
        return self

    def getItems(self):
        """
        Get all parameters and corresponding distributions.

        :return: all parameters and corresponding distributions.
        """
        return self.items
