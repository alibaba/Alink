from abc import ABC
from typing import Sequence, Union, List

import numpy as np
from py4j.java_gateway import JavaObject

from ..common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ..common.types.conversion.type_converters import py_list_to_j_array, j_value_to_py_value, flink_type_to_str
from ..common.types.file_system.file_system import FilePath
from ..py4j_util import get_java_class

__all__ = ["LocalPredictor", "LocalPredictable"]


class LocalPredictor(JavaObjectWrapper):

    def __init__(self, j_local_predictor_or_model_path: Union[JavaObject, str, FilePath], input_schema_str: str):
        j_local_predictor_cls = get_java_class("com.alibaba.alink.pipeline.LocalPredictor")
        if isinstance(j_local_predictor_or_model_path, (JavaObject, )):
            j_local_predictor = j_local_predictor_or_model_path
        elif isinstance(j_local_predictor_or_model_path, str):
            j_local_predictor = j_local_predictor_cls(j_local_predictor_or_model_path, input_schema_str)
        elif isinstance(j_local_predictor_or_model_path, FilePath):
            j_local_predictor = j_local_predictor_cls(j_local_predictor_or_model_path.get_j_obj(), input_schema_str)
        else:
            raise ValueError("Must be JavaObject, str, or FilePath")
        self._j_local_predictor = j_local_predictor
        j_csv_util_cls = get_java_class("com.alibaba.alink.common.utils.TableUtil")
        self._j_input_col_types = j_csv_util_cls.getColTypes(input_schema_str)

    def get_j_obj(self) -> JavaObject:
        return self._j_local_predictor

    def getOutputColNames(self) -> List[str]:
        return list(self.get_j_obj().getOutputSchema().getFieldNames())

    def getOutputColTypes(self) -> List[str]:
        j_coltypes = self.get_j_obj().getOutputSchema().getFieldTypes()
        return [flink_type_to_str(j_coltype) for j_coltype in j_coltypes]

    def getOutputSchemaStr(self):
        col_names = self.getOutputColNames()
        col_types = self.getOutputColTypes()
        return ", ".join([k + " " + v for k, v in zip(col_names, col_types)])

    @staticmethod
    def _create_input_j_row(values: Union[Sequence, np.ndarray], j_coltypes: List[JavaObject]) -> JavaObject:
        j_row_cls = get_java_class("org.apache.flink.types.Row")
        j_object_cls = get_java_class("java.lang.Object")
        j_values_array = py_list_to_j_array(j_object_cls, len(values), values)
        j_row = j_row_cls.of(j_values_array)

        j_row_type_adapter = get_java_class("com.alibaba.alink.python.utils.RowTypeAdapter")
        j_row_type_adapter.adjustRowTypeInplace(j_row, j_coltypes)
        return j_row

    def map(self, values: Union[Sequence, np.ndarray]) -> np.ndarray:
        if isinstance(values, (np.ndarray,)):
            if values.ndim != 1:
                raise ValueError("Only support numpy.ndarray with 1 dimension.")
            values = values.tolist()

        j_row = self._create_input_j_row(values, self._j_input_col_types)
        j_result_row = self.get_j_obj().map(j_row)
        output_size = j_result_row.getArity()
        result = np.empty(output_size, dtype=object)
        for i in range(output_size):
            result[i] = j_value_to_py_value(j_result_row.getField(i))
        return result

    def open(self):
        self.get_j_obj().open()

    def close(self):
        self.get_j_obj().close()


class LocalPredictable(JavaObjectWrapper, ABC):

    def collectLocalPredictor(self, inputSchemaStr: str) -> LocalPredictor:
        return LocalPredictor(self.get_j_obj().collectLocalPredictor(inputSchemaStr), inputSchemaStr)
