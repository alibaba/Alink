import datetime
import logging
from io import StringIO
from typing import List

from alink.py4j_gateway import gateway
from py4j.java_collections import JavaArray
from py4j.java_gateway import JavaObject

try:
    import numpy as np
    import pandas as pd
    from scipy.sparse import spmatrix, csr_matrix

    support_mtv = True
except ImportError:
    support_mtv = False

_st = datetime.datetime.utcfromtimestamp(0)

TENSOR_WRAPPER_CLASS_NAME = "com.alibaba.alink.common.pyrunner.fn.conversion.TensorWrapper"
DENSE_VECTOR_WRAPPER_CLASS_NAME = "com.alibaba.alink.common.pyrunner.fn.conversion.DenseVectorWrapper"
SPARSE_VECTOR_WRAPPER_CLASS_NAME = "com.alibaba.alink.common.pyrunner.fn.conversion.SparseVectorWrapper"
VECTOR_WRAPPER_CLASS_NAME = "com.alibaba.alink.common.pyrunner.fn.conversion.VectorWrapper"
MTABLE_WRAPPER_CLASS_NAME = "com.alibaba.alink.common.pyrunner.fn.conversion.MTableWrapper"


def j_timestamp_to_time(v: JavaObject):
    j_local_date_time = v.toLocalDateTime()
    return datetime.datetime(
        year=j_local_date_time.getYear(),
        month=j_local_date_time.getMonthValue(),
        day=j_local_date_time.getDayOfMonth(),
        hour=j_local_date_time.getHour(),
        minute=j_local_date_time.getMinute(),
        second=j_local_date_time.getSecond(),
        microsecond=int(j_local_date_time.getNano() / 1000)
    )


def datetime_to_int(v: datetime.datetime):
    return int((v - _st) / datetime.timedelta(milliseconds=1))


def j_time_to_time(v: JavaObject):
    j_local_time = v.toLocalTime()
    return datetime.time(hour=j_local_time.getHour(),
                         minute=j_local_time.getMinute(),
                         second=j_local_time.getSecond(),
                         microsecond=int(j_local_time.getNano() / 1000))


def time_to_int(v: datetime.time):
    return (v.hour * 60 * 60 + v.minute * 60 + v.second) * 1000 + int(v.microsecond / 1000)


def j_date_to_date(v: JavaObject):
    j_local_date = v.toLocalDate()
    return datetime.date(year=j_local_date.getYear(),
                         month=j_local_date.getMonthValue(),
                         day=j_local_date.getDayOfMonth())


def date_to_int(v: datetime.date):
    return (v - _st.date()).days


to_py_rules = [
    ("java.sql.Timestamp", j_timestamp_to_time),
    ("java.sql.Time", j_time_to_time),
    ("java.sql.Date", j_date_to_date),
]

to_java_rules = [
    (None, (datetime.datetime,), datetime_to_int),
    (None, (datetime.date,), date_to_int),
    (None, (datetime.time,), time_to_int),
]

if support_mtv:
    def j_tensor_wrapper_to_np_array(j_obj: JavaObject):
        buffer, dtype_str, shape = j_obj.getBytes(), j_obj.getDtypeStr(), j_obj.getShape()
        arr = np.frombuffer(buffer, dtype=dtype_str).reshape(shape)
        return arr


    def np_array_to_j_tensor_wrapper(arr: np.ndarray):
        tensor_wrapper_cls = gateway.jvm.__getattr__(TENSOR_WRAPPER_CLASS_NAME)
        j_obj = tensor_wrapper_cls.fromPy(arr.tobytes(), arr.dtype.str, arr.shape)
        return j_obj


    def j_dense_vector_wrapper_to_np_array(j_obj: JavaObject):
        buffer, size = j_obj.getBytes(), j_obj.getSize()
        arr = np.frombuffer(buffer, dtype="<f8").reshape([size])
        return arr


    def np_array_to_j_dense_vector_wrapper(arr: np.ndarray):
        dense_vector_wrapper_cls = gateway.jvm.__getattr__(DENSE_VECTOR_WRAPPER_CLASS_NAME)
        assert arr.dtype.str == "<f8", "np.array must have type np.float64/'<f8'"
        assert len(arr.shape) == 1, "np.array must be rank-1"
        j_obj = dense_vector_wrapper_cls.fromPy(arr.tobytes(), arr.shape[0])
        return j_obj


    def j_sparse_vector_wrapper_to_scipy_spmatrix(j_obj: JavaObject):
        """
        Convert a Java object of `SparseVectorWrapper` to a scipy sparse matrix whose number of rows is 1.
        `j_obj.getSize()` must return a positive number.

        NOTE:
          - To access i-th element, use `A[0, i]` rather than `A[i]`.
          - To convert the sparse matrix to a numpy.ndarray, use `A.todense()`.
          - For other usages, please refer to scipy documents.

        TODO: find a better type to represent `SparseVector` in Python side.

        :param j_obj: a Java object of `SparseVectorWrapper`
        :return: a scipy sparse matrix
        """
        indices = np.frombuffer(j_obj.getIndicesBytes(), dtype="<i4")
        values = np.frombuffer(j_obj.getValuesBytes(), dtype="<f8")
        size = j_obj.getSize()
        indptr = np.array([0, indices.shape[0]], dtype=np.int32)
        return csr_matrix((values, indices, indptr), shape=(1, size), dtype=np.float64).todok()


    def scipy_spmatrix_to_j_sparse_vector_wrapper(sv: spmatrix):
        assert sv.shape[0] == 1, \
            "The first dimension of the sparse matrix must have size 1 to be convertible to a SparseVector."
        # Convert to dok representation to get indices and values
        sv: csr_matrix = sv.tocsr()
        sparse_vector_wrapper_cls = gateway.jvm.__getattr__(SPARSE_VECTOR_WRAPPER_CLASS_NAME)
        return sparse_vector_wrapper_cls.fromPy(sv.indices.tobytes(), sv.data.tobytes(), sv.shape[1])


    def j_mtable_wrapper_to_pd_dataframe(j_obj: JavaObject):
        content: str = j_obj.getContent()
        col_names: List[str] = j_obj.getColNames()
        df = pd.read_csv(StringIO(content), names=col_names)
        return df


    def pd_dataframe_to_j_mtable_wrapper(df: pd.DataFrame):
        def convert_dtype_to_str(dtype):
            if pd.api.types.is_integer_dtype(dtype):
                return "long"
            elif pd.api.types.is_float_dtype(dtype):
                return "double"
            elif pd.api.types.is_bool_dtype(dtype):
                return "boolean"
            elif pd.api.types.is_string_dtype(dtype):
                return "string"
            logging.info("Don't know type {}, use 'string'".format(dtype))
            # TODO: need improve types conversion
            return "string"

        def get_schema_str(col_names, dtypes):
            col_types = [convert_dtype_to_str(dtypes.get(col_name)) for col_name in col_names]
            return ', '.join([col_name + ' ' + col_type for col_name, col_type in zip(col_names, col_types)])

        schema_str: str = get_schema_str(df.columns.to_numpy().tolist(), df.dtypes)
        content = df.to_csv(index=False, header=False)
        mtable_wrapper_cls = gateway.jvm.__getattr__(MTABLE_WRAPPER_CLASS_NAME)
        return mtable_wrapper_cls.fromPy(content, schema_str)


    to_py_rules.extend([
        (TENSOR_WRAPPER_CLASS_NAME, j_tensor_wrapper_to_np_array),
        (DENSE_VECTOR_WRAPPER_CLASS_NAME, j_dense_vector_wrapper_to_np_array),
        (SPARSE_VECTOR_WRAPPER_CLASS_NAME, j_sparse_vector_wrapper_to_scipy_spmatrix),
        (MTABLE_WRAPPER_CLASS_NAME, j_mtable_wrapper_to_pd_dataframe),
    ])
    to_java_rules.extend([
        ([TENSOR_WRAPPER_CLASS_NAME, 'TENSOR'], (np.ndarray,), np_array_to_j_tensor_wrapper),
        ([VECTOR_WRAPPER_CLASS_NAME, 'VECTOR'], (np.ndarray,), np_array_to_j_dense_vector_wrapper),
        ([VECTOR_WRAPPER_CLASS_NAME, 'VECTOR'], (spmatrix,), scipy_spmatrix_to_j_sparse_vector_wrapper),
        ([MTABLE_WRAPPER_CLASS_NAME, 'MTABLE'], (pd.DataFrame,), pd_dataframe_to_j_mtable_wrapper),
    ])


def to_py_value(v):
    """
    Convert the argument from Java side to pure Python one.
    :param v:
    :return:
    """
    if isinstance(v, (list, tuple, JavaArray)):
        return list(map(to_py_value, v))
    if isinstance(v, (JavaObject,)):
        j_cls_name = v.getClass().getCanonicalName()
        for rule in to_py_rules:
            if j_cls_name == rule[0]:
                return rule[1](v)
        raise ValueError("Unexpected JavaObject value of type: " + j_cls_name)
    return v


def to_java_value(v, j_cls_name: str):
    for rule in to_java_rules:
        if (rule[0] is None or j_cls_name in rule[0]) and isinstance(v, rule[1]):
            return rule[2](v)
    return v


def to_java_values(values: List, j_cls_names: List[str]):
    j_values = []
    for v, j_cls_name in zip(values, j_cls_names):
        j_values.append(to_java_value(v, j_cls_name))
    return j_values
