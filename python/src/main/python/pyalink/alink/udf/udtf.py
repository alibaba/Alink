"""
Table function-related definitions for lower versions of PyFlink.
"""

import abc
import collections
import functools
import inspect

# noinspection PyProtectedMember
from pyflink.table.types import DataType

from .compat import UserDefinedFunction


class TableFunction(UserDefinedFunction):
    """
    Base interface for user-defined table function. A user-defined table functions maps zero, one,
    or multiple scalar values to an arbitrary number of rows as output consisting of one or more values.
    """

    @abc.abstractmethod
    def eval(self, *args):
        """
        Method which defines the logic of the table function.
        """
        pass


class UserDefinedTableFunctionWrapper:
    def __init__(self, func, input_types, result_types, deterministic=None, name=None):
        if inspect.isclass(func) or (not isinstance(func, UserDefinedFunction) and not callable(func)):
            raise TypeError(
                "Invalid function: not a function or callable (__call__ is not defined): {0}".format(type(func)))

        if not isinstance(input_types, collections.Iterable):
            input_types = [input_types]

        for input_type in input_types:
            if not isinstance(input_type, DataType):
                raise TypeError(
                    "Invalid input_type: input_type should be DataType but is {}".format(
                        input_type))

        if not isinstance(result_types, collections.Iterable):
            result_types = [result_types]

        for result_type in result_types:
            if not isinstance(result_type, DataType):
                raise TypeError(
                    "Invalid result_type: result_type should be DataType but is {}".format(result_type))

        self._func = func
        self._input_types = input_types
        self._result_types = result_types
        self._name = name or (func.__name__ if hasattr(func, '__name__') else func.__class__.__name__)

        if deterministic is not None:
            print("Warning: deterministic is always False when using udf/udtf with PyAlink.")
        self._deterministic = False


def _create_udtf(f, input_types, result_types, deterministic, name):
    return UserDefinedTableFunctionWrapper(f, input_types, result_types, deterministic, name)


def udtf(f=None, input_types=None, result_types=None, deterministic=None, name=None):
    """
    Helper method for creating a user-defined table function.


    :param f: lambda function or user-defined function.
    :type f: function or UserDefinedFunction or type
    :param input_types: the input data types.
    :type input_types: list[DataType] or DataType
    :param result_types: the result data type.
    :type result_types: list[DataType] or DataType
    :param name: the function name.
    :type name: str
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters. (default False)
    :type deterministic: bool
    :return: UserDefinedTableFunctionWrapper or function.
    :rtype: UserDefinedTableFunctionWrapper or function
    """
    # decorator
    if f is None:
        return functools.partial(_create_udtf, input_types=input_types, result_types=result_types,
                                 deterministic=deterministic, name=name)
    else:
        return _create_udtf(f, input_types, result_types, deterministic, name)
