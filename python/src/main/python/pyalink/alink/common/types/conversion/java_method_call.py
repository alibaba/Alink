import functools

from .type_converters import py_obj_to_j_obj, j_value_to_py_value, py_list_to_j_array
from ....py4j_util import get_java_class

__all__ = ['call_java_method', 'auto_convert_java_type']

j_type_candidates = dict()

j_type_candidates[int] = [
    "java.lang.Integer",
    "java.lang.Long",
    "int",
    "long"
]
j_type_candidates[float] = [
    "java.lang.Float",
    "java.lang.Double",
    "float",
    "double",
]
j_type_candidates[str] = ["java.lang.String"]

all_candidates = [*j_type_candidates[int], *j_type_candidates[float], *j_type_candidates[str]]


def call_java_method_recursive(func, *args):
    """
    Call function `func` with arguments `args`.

    The calling process is recursively done by creating a partial function with the first argument if any.
    Otherwise, directly call `func` without arguments.

    Some arguments are Python lists, and have to be converted to Java arrays.
    As there are no good ways to know the actual array types, all possible candidates of Java types are tested,
    in a recursive way.
    So the function call may be failed, and the status is indicated in the return value.

    :param func: function
    :param args: arguments
    :return: a tuple indicating whether the function is called successfully, and the return value upon success.
    """
    def _get_first_elem(lst_or_item):
        if not isinstance(lst_or_item, (list, tuple,)):
            return lst_or_item
        retval = None
        for item in lst_or_item:
            retval = _get_first_elem(item)
            if not retval:
                break
        return retval

    if len(args) == 0:
        try:
            return True, func()
        except Exception as ex:
            # noinspection PyProtectedMember
            from ....config import g_config
            debug_mode = g_config["debug_mode"]
            if debug_mode:
                print(ex)
            return False, None

    arg = args[0]
    if not isinstance(arg, (list, tuple,)):
        partial_f = functools.partial(func, arg)
        return call_java_method_recursive(partial_f, *args[1:])
    else:
        first_elem = _get_first_elem(arg)
        if first_elem is not None:
            # TODO: fix for arrays of non-primitive types
            arg_type = type(first_elem)
            candidates = j_type_candidates.get(arg_type, all_candidates)
        else:
            candidates = all_candidates
        candidates = [*candidates, "Object"]

        for candidate in candidates:
            j_type = get_java_class(candidate)
            converted_arg = py_list_to_j_array(j_type, len(arg), arg)
            partial_f = functools.partial(func, converted_arg)
            success, v = call_java_method_recursive(partial_f, *args[1:])
            if success:
                return True, v
        return False, None


def call_java_method(f, *args):
    """
    Call Java method `f` with arguments `args`.

    All `args` are first converted to Java objects (py4j `JavaObject`).
    Then, the Java method is called, and the return value (py4j `JavaObject`) is converted to Python values.

    In the conversion of arguments, only `JavaWrapper` are unwrapped to Java objects (see `py_obj_to_j_obj`).
    Python lists are converted to Java arrays in :py:func:`call_j_method_recursive`.

    In the conversion of return value, some Java objects are wrapped to `JavaWrapper` following some rules,
    and arrays are converted to Python lists (see `j_value_to_py_value`).

    :param f: the Java method
    :param args: arguments
    :return: return value of the Java method, converted to Python types
    """
    args = list(map(py_obj_to_j_obj, args))
    has_iterable = any([isinstance(arg, (list, tuple,)) for arg in args])
    if not has_iterable:
        retval = f(*args)
    else:
        success, retval = call_java_method_recursive(f, *args)
        if not success:
            raise Exception("Cannot call Java method " + f.__name__ + " with args: ", args)
    py_v = j_value_to_py_value(retval)
    return py_v


def auto_convert_java_type(f):
    """
    A decorator on functions which transforms arguments from Python types to Java types
    and the return value from Java types to Python types.
    :param f: function
    :return: the return value
    """

    def decorated_f(*args):
        return call_java_method(f, *args)

    return decorated_f
