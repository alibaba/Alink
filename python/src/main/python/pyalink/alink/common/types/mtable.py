import os
from typing import List, Any, Union, Optional, Sequence

import pandas
from py4j.java_gateway import JavaObject

from .bases.j_obj_wrapper import JavaObjectWrapper
from .conversion.java_method_call import auto_convert_java_type, call_java_method
from .conversion.type_converters import csv_content_to_dataframe
from .data_type_display import DataTypeDisplay
from .stat_summary import TableSummary
from ...py4j_util import get_java_class

__all__ = ['MTable']


class MTable(JavaObjectWrapper, DataTypeDisplay):
    """
    A Python-equivalent to Java `MTable`. Some notes:

    - Column types are mandatory in Python's definition.
    - Setters/getters automatically convert Python values from/to Java values.
    - Not support methods like `select`, `orderBy`, `reduceGroup`, etc. You can use APIs from `DataFrame`.
    """
    _j_cls_name = 'com.alibaba.alink.common.MTable'

    def __init__(self, j_mtable: JavaObject):
        self._df: Optional[pandas.DataFrame] = None
        self._j_mtable = j_mtable
        self._update_df()

    def get_j_obj(self) -> JavaObject:
        return self._j_mtable

    def _update_df(self):
        """
        Update field `_df` based on `_j_mtable`.
        """
        j_operator_csv_collector_cls = get_java_class('com.alibaba.alink.python.utils.OperatorCsvCollector')
        line_terminator = os.linesep
        field_delimiter = ","
        quote_char = "\""
        csv_content = j_operator_csv_collector_cls.mtableToCsv(self.get_j_obj(), line_terminator, field_delimiter,
                                                               quote_char)
        self._df = csv_content_to_dataframe(csv_content, self.getColNames(), self.getColTypes())

    @staticmethod
    def fromDataframe(df: pandas.DataFrame, schemaStr: str) -> 'MTable':
        """
        Construct a :py:class:`MTable` instance from a :py:class:`pandas.DataFrame` instance.

        :param df: the :py:class:`pandas.DataFrame` instance.
        :param schemaStr: schema string.
        :return: a :py:class:`MTable` instance.
        """
        from .conversion.type_converters import dataframe_to_operator
        return dataframe_to_operator(df, schemaStr, 'mtable')

    def toDataframe(self) -> pandas.DataFrame:
        """
        Convert `self` to a :py:class:`pandas.DataFrame` instance.

        :return: a :py:class:`pandas.DataFrame` instance.
        """
        return self._df

    def getRows(self) -> pandas.DataFrame:
        """
        Get all rows as a :py:class:`pandas.DataFrame` instance. Same as :py:func:`toDataframe`.

        :return: a :py:class:`pandas.DataFrame` instance.
        """
        return self._df

    def getRow(self, index: int) -> pandas.DataFrame:
        """
        Get one row as a :py:class:`pandas.DataFrame` instance with only one row.

        :return: a :py:class:`pandas.DataFrame` instance.
        """
        return self._df.iloc[[index]]

    def getColNames(self) -> List[str]:
        return list(self.get_j_obj().getColNames())

    def getColTypes(self) -> List[str]:
        FlinkTypeConverter = get_java_class("com.alibaba.alink.operator.common.io.types.FlinkTypeConverter")
        coltypes = self.get_j_obj().getColTypes()
        return [str(FlinkTypeConverter.getTypeString(i)) for i in coltypes]

    def getSchemaStr(self) -> str:
        col_names = self.getColNames()
        col_types = self.getColTypes()
        return ", ".join([k + " " + v for k, v in zip(col_names, col_types)])

    @auto_convert_java_type
    def getNumRow(self) -> int:
        """
        Get number of rows.

        :return: number of rows.
        """
        return self.getNumRow()

    @auto_convert_java_type
    def getNumCol(self):
        """
        Get number of columns.

        :return: number of columns.
        """
        return self.getNumCol()

    @auto_convert_java_type
    def getEntry(self, row: int, col: int) -> Any:
        """
        Get entry by row index and column index.

        :param row: row index.
        :param col: column index.
        :return: value.
        """
        return self.getEntry(row, col)

    def setEntry(self, row: int, col: int, value: Any) -> None:
        """
        Set entry by row index and column index.

        Different from Java side, exception is thrown when value type is not consistent.

        :param row: row index.
        :param col: column index.
        :param value: new value.
        """
        call_java_method(self.get_j_obj().setEntry, row, col, value)
        self._update_df()

    @auto_convert_java_type
    def select(self, selectedColNamesOrIndices: Union[Sequence[str], Sequence[int]]) -> 'MTable':
        """
        Construct a :py:class:`pandas.DataFrame` instance with selected columns.

        :param selectedColNamesOrIndices: selected column names or indices.
        :return: a :py:class:`pandas.DataFrame` instance.
        """
        return self.select(selectedColNamesOrIndices)

    @auto_convert_java_type
    def summary(self, selectedColNames: Sequence[str]) -> TableSummary:
        """
        Get statistic summary of selected columns.

        :param selectedColNames: selected column names.
        :return: statistic summary.
        """
        return self.summary(selectedColNames)

    @auto_convert_java_type
    def subSummary(self, selectedColNames: Sequence[str], fromId: int, endId: int) -> TableSummary:
        """
        Get statistic summary of selected columns over a range of rows.

        :param selectedColNames: selected column names.
        :param fromId: start index of rows (inclusive).
        :param endId:  end index of rows (exclusive).
        :return: statistic summary.
        """
        return self.subSummary(selectedColNames, fromId, endId)

    @auto_convert_java_type
    def orderBy(self, fields_or_indices: Union[Sequence[str], Sequence[int]], orders: Sequence[bool] = None) -> None:
        """
        Sort rows by fields or indices in place.

        :param fields_or_indices: fields or indices for comparison.
        :param orders: orders for each fields. `True` for ascending, and `False` for descending.
        """
        if orders is None:
            return self.orderBy(fields_or_indices)
        else:
            return self.orderBy(fields_or_indices, orders)

    @auto_convert_java_type
    def toDisplayData(self, n: int = None):
        if n is None:
            return self.toDisplayData()
        else:
            return self.toDisplayData(n)

    @auto_convert_java_type
    def toDisplaySummary(self) -> str:
        return self.toDisplaySummary()

    @auto_convert_java_type
    def toShortDisplayData(self) -> str:
        return self.toShortDisplayData()

    @auto_convert_java_type
    def toJson(self) -> str:
        """
        Convert `self` to a JSON string.

        :return: a JSON string.
        """
        return self.toJson()

    @staticmethod
    def fromJson(s: str) -> 'MTable':
        """
        Parse a JSON string to a `MTable` instance.

        :param s: a JSON string.
        :return: a `MTable` instance.
        """
        return MTable(MTable._j_cls().fromJson(s))

    @auto_convert_java_type
    def copy(self) -> 'MTable':
        return self.copy()

    _unsupported_j_methods = ['readCsvFromFile', 'writeCsvToFile', 'sampleWithSizeReplacement',
                              'sampleWithSize', 'reduceGroup', 'getSchema', 'printSummary', 'subTable']
