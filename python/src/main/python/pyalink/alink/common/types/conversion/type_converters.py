import deprecation
import numpy as np
import os
import pandas as pd

from ....py4j_util import get_java_class

# Basic type conversion
_G_ALINK_TYPE_TO_PTYPE = {
    'BOOL': 'bool',
    'BOOLEAN': 'bool',
    'JAVA.LANG.BOOLEAN': 'bool',

    'TINYINT': 'Int8',
    'BYTE': 'Int8',
    'JAVA.LANG.BYTE': 'Int8',

    'SMALLINT': 'Int16',
    'JAVA.LANG.SHORT': 'Int16',

    'INT': 'Int32',
    'INTEGER': 'Int32',
    'JAVA.LANG.INTEGER': 'Int32',

    'BIGINT': 'Int64',
    'LONG': 'Int64',
    'JAVA.LANG.LONG': 'Int64',

    'FLOAT': 'float32',
    'JAVA.LANG.FLOAT': 'float32',

    'DOUBLE': 'float64',
    'JAVA.LANG.DOUBLE': 'float64',

    'STRING': 'object',
    'VARCHAR': 'object',
    'LONGVARCHAR': 'object',
    'JAVA.LANG.STRING': 'object',

    'DATETIME': 'datetime64',
    'JAVA.SQL.TIMESTAMP': 'datetime64',
    'TIMESTAMP': 'datetime64',
}


def j_type_to_py_type(t):
    typeclass = t.getTypeClass()
    typeclass_name = typeclass.getName()
    if typeclass_name in ['java.lang.Double', 'java.lang.Float', 'double', 'float']:
        return np.float64
    elif typeclass_name in ['java.lang.Long', 'java.lang.Integer', 'int', 'long']:
        return pd.Int64Dtype()
    elif typeclass_name == 'java.lang.String':
        return np.object
    elif typeclass_name == 'java.sql.Timestamp':
        return np.datetime64
    elif typeclass_name == "com.alibaba.alink.common.linalg.Vector" or typeclass_name == "com.alibaba.alink.common.linalg.DenseVector" or typeclass_name == "com.alibaba.alink.common.linalg.SparseVector":
        return np.str
    elif typeclass_name in ["java.lang.Boolean", 'boolean']:
        return np.bool
    else:
        print("Java type is not supported in Python for automatic conversion of values: %s" % typeclass_name)
        return t


def flink_type_to_str(t):
    j_flink_type_converter_cls = get_java_class("com.alibaba.alink.operator.common.io.types.FlinkTypeConverter")
    return j_flink_type_converter_cls.getTypeString(t)


# basic value conversion

j_obj_to_py_obj_rules = None


def get_all_subclasses(cls):
    """
    Get all subclasses of a given class. Note that the results will depend on current imports.

    :param cls: a class.
    :return: the set of all subclasses.
    """
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in get_all_subclasses(c)])


def get_j_obj_to_py_obj_rules():
    """
    Provides rules to transform from Java Objects to Python objects.
    Batch/StreamOperators are not included.
    :return:
    """

    # noinspection PyProtectedMember
    from ..vector import VectorIterator

    from pyflink.table.catalog import ObjectPath
    rules = {
        "org.apache.flink.api.java.tuple.Tuple2": lambda v: (j_value_to_py_value(v.f0), j_value_to_py_value(v.f1)),
        'com.alibaba.alink.common.linalg.DenseVector$DenseVectorIterator': VectorIterator,
        'com.alibaba.alink.common.linalg.SparseVector$SparseVectorVectorIterator': VectorIterator,
        'org.apache.flink.table.catalog.ObjectPath': lambda v: ObjectPath(j_object_path=v)
    }
    from ..bases.j_obj_wrapper import JavaObjectWrapper
    py_classes = get_all_subclasses(JavaObjectWrapper)
    for py_cls in py_classes:
        if hasattr(py_cls, '_j_cls_name'):
            # noinspection PyProtectedMember
            rules[py_cls._j_cls_name] = py_cls
    return rules


def j_obj_to_py_obj(j_inst):
    global j_obj_to_py_obj_rules
    if j_obj_to_py_obj_rules is None:
        j_obj_to_py_obj_rules = get_j_obj_to_py_obj_rules()
    j_cls_name = j_inst.getClass().getName()
    if j_cls_name in j_obj_to_py_obj_rules:
        return j_obj_to_py_obj_rules.get(j_cls_name)(j_inst)
    return j_inst


def j_value_to_py_value(value):
    from py4j.java_collections import JavaArray
    from py4j.java_gateway import JavaObject
    from py4j.java_collections import JavaMap
    if isinstance(value, JavaArray):
        return j_array_to_py_list(value)
    elif isinstance(value, JavaMap):
        return j_map_to_py_dict(value)
    elif isinstance(value, JavaObject):
        return j_obj_to_py_obj(value)
    return value


def py_obj_to_j_obj(value):
    from ..bases.j_obj_wrapper import JavaObjectWrapper
    from pyflink.table.catalog import ObjectPath
    if isinstance(value, (JavaObjectWrapper,)):
        return value.get_j_obj()
    elif isinstance(value, ObjectPath):
        # noinspection PyProtectedMember
        return value._j_object_path
    return value


# java array <-> python list

def j_array_to_py_list(arr):
    lst = [j_value_to_py_value(d) for d in arr]
    return lst


def j_map_to_py_dict(j_map):
    d = dict()
    for entry in j_map.entrySet():
        key = j_value_to_py_value(entry.getKey())
        value = j_value_to_py_value(entry.getValue())
        d[key] = value
    return d


def py_list_to_j_array(j_type, num, items):
    """
    Convert Python list `items` to Java array of Java type `j_type`.

    The list `items` can be nested, in which cases multi-dimensional array is returned.
    When `items` is a nested list, levels of all elements must be same. For example, [[1, 2], 3] is illegal.
    :param j_type: the Java type of elements
    :param num: number of elements in the list
    :param items: the list of elements
    :return: the Java array
    """

    def get_nested_levels(lst):
        n_level = 0
        while isinstance(lst, (list, tuple,)):
            n_level += 1
            if len(lst) == 0:
                break
            lst = lst[0]
        return n_level

    return py_list_to_j_array_nd(j_type, num, items, get_nested_levels(items))


def py_list_to_j_array_nd(j_type, num, items, n_dims):
    if n_dims == 1:
        j_items = items
        # noinspection PyProtectedMember
        j_elem_type_cls = j_type._java_lang_class
    else:
        j_items = [
            py_list_to_j_array_nd(j_type, len(item), item, n_dims - 1)
            for item in items
        ]
        j_elem_type_cls = j_items[0].getClass()

    j_arr = get_java_class("java.lang.reflect.Array").newInstance(j_elem_type_cls, num)
    for i, j_item in enumerate(j_items):
        j_arr[i] = j_item
    return j_arr


# Flink rows <-> pd dataframe

def schema_type_to_py_type(raw_type):
    t = raw_type.upper()
    if t in _G_ALINK_TYPE_TO_PTYPE:
        return _G_ALINK_TYPE_TO_PTYPE[t]
    else:
        print("Java type is not supported in Python for automatic conversion of values: %s" % t)
        return np.object


def adjust_dataframe_types(df, colnames, coltypes):
    for (colname, coltype) in zip(colnames, coltypes):
        col = df[colname]
        py_type = schema_type_to_py_type(coltype)
        if not pd.api.types.is_float_dtype(py_type) \
                and not pd.api.types.is_integer_dtype(py_type) \
                and col.isnull().values.any():
            print("Warning: null values exist in column %s, making it cannot be cast to type: %s automatically" % (
                colname, str(coltype)))
            continue
        df = df.astype({colname: py_type}, copy=False, errors='ignore')
    return df


# operator(s) -> dataframe(s)

def post_convert(df: pd.DataFrame, colnames, coltypes):
    """
    In :py:func:`csv_content_to_dataframe`, some user-defined types are read as strings.
    They need to be converted to real types.
    """
    from ..mtable import MTable
    from ..tensor import Tensor

    def to_tensor(s: str):
        j_tensor_util_cls = get_java_class("com.alibaba.alink.common.linalg.tensor.TensorUtil")
        return Tensor(j_tensor_util_cls.getTensor(s))

    # Different from `j_value_to_py_value`, values in df may be `str`, but needs to be converted to other types.
    converters = {
        'ANY<com.alibaba.alink.common.MTable>'.upper(): lambda s: MTable.fromJson(s)
    }
    for tensor_cls in get_all_subclasses(Tensor):
        # noinspection PyProtectedMember
        key = ('ANY<' + tensor_cls._j_cls_name + '>').upper()
        converters[key] = to_tensor

    for colname, coltype in zip(colnames, coltypes):
        if coltype.upper() in converters:
            df[colname] = df[colname].apply(converters[coltype.upper()])
    return df


def csv_content_to_dataframe(content, colnames, coltypes):
    from io import StringIO
    force_dtypes = {
        colname: _G_ALINK_TYPE_TO_PTYPE.get(coltype, 'object')
        for colname, coltype in zip(colnames, coltypes)
        if coltype in ["VARCHAR", "STRING"]
    }
    date_colnames = [
        colname
        for colname, coltype in zip(colnames, coltypes)
        if coltype in ["TIMESTAMP", 'JAVA.SQL.TIMESTAMP', 'DATETIME']
    ]
    df = pd.read_csv(StringIO(content), names=colnames, dtype=force_dtypes, parse_dates=date_colnames,
                     true_values=["True", "true"], false_values=["False", "false"])
    # As all empty strings are read as NaN, we transform them to None
    df = df.where(df.notnull(), None)
    # For float/int columns, there are specialized types to represent values with null values, we adjust their types
    df = adjust_dataframe_types(df, colnames, coltypes)
    return post_convert(df, colnames, coltypes)


def collect_to_dataframes_memory(*ops):
    j_batch_operator_class = get_java_class('com.alibaba.alink.operator.batch.BatchOperator')
    j_op_list = py_list_to_j_array(j_batch_operator_class, len(ops), list(map(lambda op: op.get_j_obj(), ops)))

    line_terminator = os.linesep
    field_delimiter = ","
    quote_char = "\""

    j_operator_csv_collector_cls = get_java_class('com.alibaba.alink.python.utils.OperatorCsvCollector')
    csv_contents = j_operator_csv_collector_cls.collectToCsv(j_op_list, line_terminator, field_delimiter, quote_char)

    return [
        csv_content_to_dataframe(content, ops[index].getColNames(), ops[index].getColTypes())
        for index, content in enumerate(csv_contents)
    ]


def lazy_collect_to_dataframes(*ops):
    j_batch_operator_class = get_java_class('com.alibaba.alink.operator.batch.BatchOperator')
    j_op_list = py_list_to_j_array(j_batch_operator_class, len(ops), list(map(lambda op: op.get_j_obj(), ops)))

    line_terminator = os.linesep
    field_delimiter = ","
    quote_char = "\""

    j_operator_csv_collector_cls = get_java_class('com.alibaba.alink.python.utils.OperatorCsvCollector')
    j_lazy_csv_contents = j_operator_csv_collector_cls.lazyCollectToCsv(j_op_list, line_terminator, field_delimiter,
                                                                        quote_char)

    from ....batch.lazy_evaluation import LazyEvaluation, PipeLazyEvaluationConsumer
    lazy_dfs = []
    for index, j_lazy_csv_content in enumerate(j_lazy_csv_contents):
        lazy_csv_content = LazyEvaluation()
        lazy_df = lazy_csv_content \
            .transform(
            lambda content: csv_content_to_dataframe(content, ops[index].getColNames(), ops[index].getColTypes()))
        j_lazy_csv_content.addCallback(PipeLazyEvaluationConsumer(lazy_csv_content))
        lazy_dfs.append(lazy_df)
    return lazy_dfs


def collect_to_dataframes(*ops):
    if len(ops) == 0:
        return []
    return collect_to_dataframes_memory(*ops)


def collectToDataframes(*ops):
    return collect_to_dataframes(*ops)


# dataframe(s) ->  operator(s)

def dataframe_to_operator_memory(df, schema_str, op_type):
    j_csv_util_cls = get_java_class("com.alibaba.alink.common.utils.TableUtil")
    j_col_types = j_csv_util_cls.getColTypes(schema_str)

    df_copy = df.copy()
    for index, col_name in enumerate(df_copy.columns):
        j_col_type = j_col_types[index]
        # If the column is bool type, we need to convert 'True' to 'true', and 'False' to 'false'
        if j_col_type.toString() == "Boolean":
            df_copy[col_name] = df_copy[col_name].apply(lambda x: x if x is None else str(x).lower())

    # Must set date_format, so Timestamp#valueOf in Java side works.
    content = df_copy.to_csv(index=False, header=False, date_format="%Y-%m-%d %H:%M:%S.%f")
    j_multi_line_csv_parser = get_java_class("com.alibaba.alink.python.utils.MultiLineCsvParser")

    line_terminator = os.linesep
    field_delimiter = ","
    quote_char = "\""

    if op_type == "batch":
        j_op = j_multi_line_csv_parser.csvToBatchOperator(content, schema_str,
                                                          line_terminator, field_delimiter, quote_char)
        from ....batch.base import BatchOperatorWrapper
        wrapper = BatchOperatorWrapper
    elif op_type == "stream":
        j_op = j_multi_line_csv_parser.csvToStreamOperator(content, schema_str,
                                                           line_terminator, field_delimiter, quote_char)
        from ....stream.base import StreamOperatorWrapper
        wrapper = StreamOperatorWrapper
    elif op_type == "mtable":
        j_op = j_multi_line_csv_parser.csvToMTable(content, schema_str,
                                                   line_terminator, field_delimiter, quote_char)
        from ..mtable import MTable
        wrapper = MTable
    else:
        raise ValueError("Not support op_type {}.".format(op_type))
    return wrapper(j_op)


def dataframe_to_operator(df, schema_str, op_type):
    """
    Convert a dataframe to a batch operator in alink.
    If null values exist in df, it is better to provide schema_str, so that the operator can have correct type information.

    :param schema_str: column schema string, like "col1 string, col2 int, col3 boolean"
    :param op_type:
    :param df:
    :return:
    """
    return dataframe_to_operator_memory(df, schema_str, op_type)


@deprecation.deprecated("1.3.0", details="Use BatchOperator.fromDataFrame() or StreamOperator.fromDataframe() instead.")
def dataframeToOperator(df, schemaStr, op_type=None, opType=None):
    if opType is None:
        opType = op_type
    if opType not in ["batch", "stream"]:
        raise 'opType %s not supported, please use "batch" or "stream"' % opType
    return dataframe_to_operator(df, schemaStr, opType)
