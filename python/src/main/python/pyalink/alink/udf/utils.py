import base64
import json

# noinspection PyProtectedMember
from pyflink.table.types import _to_java_type

from .compat import UserDefinedScalarFunctionWrapper, UserDefinedTableFunctionWrapper
from .data_types import AlinkDataType
from ..common.types.conversion.java_method_call import call_java_method
from ..common.utils.encoding import ensure_unicode
from ..py4j_util import get_java_class


def _to_cloudpickle_base64(v):
    import cloudpickle
    return ensure_unicode(base64.b64encode(cloudpickle.dumps(v)))


def _to_flink_type_string(data_type):
    FlinkTypeConverter = get_java_class("com.alibaba.alink.operator.common.io.types.FlinkTypeConverter")
    if isinstance(data_type, (AlinkDataType,)):
        type_string = data_type.to_type_string()
    else:
        type_string = FlinkTypeConverter.getTypeString(_to_java_type(data_type))
    mapping = {
        "TINYINT": "BYTE",
        "SMALLINT": "SHORT",
        "VARCHAR": "STRING",
    }
    if type_string in mapping:
        type_string = mapping[type_string]
    return type_string


def _java_register_function(name, j_func, env_type):
    if env_type not in ('batch', 'stream'):
        raise ValueError("env_type should be 'batch' or 'stream'")
    if env_type == "batch":
        from pyalink.alink import env
        # noinspection PyProtectedMember
        env._mlenv.btenv._j_tenv.registerFunction(name, j_func)
    elif env_type == "stream":
        from pyalink.alink import env
        # noinspection PyProtectedMember
        env._mlenv.stenv._j_tenv.registerFunction(name, j_func)


# noinspection PyProtectedMember
def register_pyflink_function(name, func, env_type):
    if isinstance(func, UserDefinedScalarFunctionWrapper):
        result_type = _to_flink_type_string(func._result_type)
        j_func = _to_judf(name, func._func, result_type, 'CLOUDPICKLE_BASE64')
    elif isinstance(func, UserDefinedTableFunctionWrapper):
        result_types = list(map(_to_flink_type_string, func._result_types))
        j_func = _to_judtf(name, func._func, result_types, 'CLOUDPICKLE_BASE64')
    else:
        raise ValueError("func should be a UserDefinedFunctionWrapper or a UserDefinedTableFunctionWrapper")
    _java_register_function(name, j_func, env_type)


def _to_judf(name, func, result_type, class_object_type):
    if not callable(func) and not hasattr(func, "eval"):
        raise ValueError("Invalid function: not a callable or an object with attr eval")

    if class_object_type == "CLOUDPICKLE_BASE64":
        class_object = _to_cloudpickle_base64(func)
    else:
        raise NotImplementedError

    config = {
        "classObject": class_object,
        "classObjectType": class_object_type
    }
    config_json = json.dumps(config)
    j_python_udf_factory_cls = get_java_class("com.alibaba.alink.common.pyrunner.fn.PyFnFactory")
    return j_python_udf_factory_cls.makeScalarFn(name, result_type, config_json)


def _to_judtf(name, func, result_types, class_object_type):
    if not callable(func) and not hasattr(func, "eval"):
        raise ValueError("Invalid function: not a callable or an object with attr eval")

    if class_object_type == "CLOUDPICKLE_BASE64":
        class_object = _to_cloudpickle_base64(func)
    else:
        raise NotImplementedError

    config = {
        "language": "python",
        "classObject": class_object,
        "classObjectType": class_object_type,
        "resultType": ','.join(result_types)
    }
    config_json = json.dumps(config)
    j_python_udf_factory_cls = get_java_class("com.alibaba.alink.common.pyrunner.fn.PyFnFactory")
    return call_java_method(j_python_udf_factory_cls.makeTableFn,
                            name, config_json, list(result_types))


# noinspection PyProtectedMember
def do_set_op_udf(op, func):
    if (callable(func) or hasattr(func, "eval")) and not isinstance(func, UserDefinedScalarFunctionWrapper):
        op.setClassObject(_to_cloudpickle_base64(func))
        op.setClassObjectType("CLOUDPICKLE_BASE64")
        return op

    if isinstance(func, UserDefinedScalarFunctionWrapper):
        result_type = _to_flink_type_string(func._result_type)
        op.setResultType(result_type) \
            .setClassObject(_to_cloudpickle_base64(func._func)) \
            .setClassObjectType("CLOUDPICKLE_BASE64")
        return op
    raise ValueError("Invalid function: not a callable, an object with attr eval, or a pyflink udf object")


# noinspection PyProtectedMember
def do_set_op_udtf(op, func):
    if (callable(func) or hasattr(func, "eval")) and not isinstance(func, UserDefinedTableFunctionWrapper):
        op.setClassObject(_to_cloudpickle_base64(func))
        op.setClassObjectType("CLOUDPICKLE_BASE64")
        return op

    if isinstance(func, UserDefinedTableFunctionWrapper):
        result_types = list(map(_to_flink_type_string, func._result_types))
        op.setResultTypes(result_types) \
            .setClassObject(_to_cloudpickle_base64(func._func)) \
            .setClassObjectType("CLOUDPICKLE_BASE64")
        return op
    raise ValueError("Invalid function: not a callable, an object with attr eval")
