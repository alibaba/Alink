from .common import PyScalarFnBatchOp as _PyScalarFnBatchOp
from .common import PyTableFnBatchOp as _PyTableFnBatchOp
from ..batch import BatchOperator
from ..py4j_util import get_java_class
from ..udf.utils import do_set_op_udf, do_set_op_udtf

__all__ = ['UDFBatchOp', 'UDTFBatchOp', 'TableSourceBatchOp']


class UDFBatchOp(_PyScalarFnBatchOp):
    """
    Similar as `UDFBatchOp` in Java side, except for supporting Python udf other than Java udf.
    """

    def __init__(self, *args, **kwargs):
        """"""
        super(UDFBatchOp, self).__init__(*args, **kwargs)

    def setFunc(self, func):
        """
        Set UDF function

        :param func: an instance with `eval` attribute, or `callable`, or an instance of :py:class:`UserDefinedScalarFunctionWrapper`.
        :return: `self`.
        """
        return do_set_op_udf(self, func)


class UDTFBatchOp(_PyTableFnBatchOp):
    """
    Similar as `UDTFBatchOp` in Java side, except for supporting Python udtf other than Java udtf.
    """

    def __init__(self, *args, **kwargs):
        """"""
        super(UDTFBatchOp, self).__init__(*args, **kwargs)

    def setFunc(self, func):
        """
        Set UDTF function

        :param func: an instance with `eval` attribute, or `callable`, or an instance of :py:class:`UserDefinedTableFunctionWrapper`.
        :return: `self`.
        """
        return do_set_op_udtf(self, func)


class TableSourceBatchOp(BatchOperator):
    """
    Convert a PyFlink :py:class:`Table` to a :py:class:`BatchOperator`.
    """

    def __init__(self, table, *args, **kwargs):
        """
        Construct a :py:class:`BatchOperator` from a PyFlink :py:class:`Table`.

        :param table: an instance of PyFlink :py:class:`Table`.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        from pyflink.table import Table
        if not isinstance(table, Table):
            raise ValueError("Invalid table: only accept PyFlink Table")

        table_source_batch_op_cls = get_java_class("com.alibaba.alink.operator.batch.source.TableSourceBatchOp")
        # noinspection PyProtectedMember
        j_op = table_source_batch_op_cls(table._j_table)
        super(TableSourceBatchOp, self).__init__(j_op=j_op, *args, **kwargs)
