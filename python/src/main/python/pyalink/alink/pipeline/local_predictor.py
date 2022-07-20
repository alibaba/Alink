from abc import ABC
from typing import Sequence, Union, List

import numpy as np
from py4j.java_gateway import JavaObject

from ..common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ..common.types.conversion.java_method_call import call_java_method
from ..common.types.conversion.type_converters import py_list_to_j_array, j_value_to_py_value, flink_type_to_str
from ..py4j_util import get_java_class

__all__ = ["LocalPredictor", "LocalPredictable"]


class LocalPredictor(JavaObjectWrapper):
    _j_cls_name = "com.alibaba.alink.pipeline.LocalPredictor"

    def __init__(self, *args):
        """
        Construct `LocalPredictor` from arguments with a wrapped Java instance.
        Different combinations of arguments are supported:

        1. j_obj: JavaObject -> directly wrap the instance, however the constructed Python instance cannot be used for map/predict;
        2. j_obj: JavaObject, inputSchemaStr: str -> directly wrap the instance, and parse input data schema;
        3. model_path: str, inputSchemaStr: str -> call `LocalPredictor(String modelPath, String inputSchemaStr)` of Java side;
        4. model_path: :py:class:`FilePath`, inputSchemaStr: str -> call `LocalPredictor(FilePath modelPath, String inputSchemaStr)` of Java side;
        5. model_path: str, inputSchemaStr: str, params: :py:class:`Params` -> call `LocalPredictor(String modelPath, String inputSchemaStr, Params params)` of Java side;
        6. model_path: :py:class:`FilePath`, inputSchemaStr: str, params: :py:class:`Params` -> call `LocalPredictor(FilePath modelPath, String inputSchemaStr, Params params)` of Java side.

        :param args: arguments, see function description.
        """
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        elif len(args) >= 2:
            if isinstance(args[0], JavaObject):
                j_obj = args[0]
            else:
                j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
            j_csv_util_cls = get_java_class("com.alibaba.alink.common.utils.TableUtil")
            self._j_input_col_types = j_csv_util_cls.getColTypes(args[1])
        else:
            raise ValueError("Must provide model path and input data schema.")
        self._j_local_predictor = j_obj

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

        values = [
            v if not isinstance(v, (JavaObjectWrapper,)) else v.get_j_obj()
            for v in values
        ]
        j_values_array = py_list_to_j_array(j_object_cls, len(values), values)
        j_row = j_row_cls.of(j_values_array)

        j_row_type_adapter = get_java_class("com.alibaba.alink.python.utils.RowTypeAdapter")
        j_row_type_adapter.adjustRowTypeInplace(j_row, j_coltypes)
        return j_row

    @staticmethod
    def _j_row_to_arr(j_row: JavaObject) -> np.ndarray:
        output_size = j_row.getArity()
        result = np.empty(output_size, dtype=object)
        for i in range(output_size):
            result[i] = j_value_to_py_value(j_row.getField(i))
        return result

    def map(self, values: Union[Sequence, np.ndarray]) -> np.ndarray:
        if isinstance(values, (np.ndarray,)):
            if values.ndim != 1:
                raise ValueError("Only support numpy.ndarray with 1 dimension.")
            values = values.tolist()

        j_row = self._create_input_j_row(values, self._j_input_col_types)
        j_result_row = self.get_j_obj().map(j_row)
        return self._j_row_to_arr(j_result_row)

    def predict(self, values: Union[Sequence, np.ndarray]) -> np.ndarray:
        return self.map(values)

    def open(self):
        self.get_j_obj().open()

    def close(self):
        self.get_j_obj().close()


class LocalPredictable(JavaObjectWrapper, ABC):

    def collectLocalPredictor(self, inputSchemaStr: str) -> LocalPredictor:
        return LocalPredictor(self.get_j_obj().collectLocalPredictor(inputSchemaStr), inputSchemaStr)
