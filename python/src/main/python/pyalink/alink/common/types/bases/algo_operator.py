from abc import abstractmethod
from typing import Optional, List, Union

from py4j.java_gateway import JavaObject
from pyflink.table import Table

from .params import Params
from .with_params import WithParams
from ....common.types.bases.j_obj_wrapper import JavaObjectWrapper
from ....py4j_util import get_java_class

__all__ = ['AlgoOperator']


class AlgoOperator(WithParams):
    """
    The base class of :py:class:`BatchOperator` and :py:class:`StreamOperator`.
    Its instance wraps a Java object of type `AlgoOperator`.
    """

    _inner_counter = 0

    def __init__(self, params: Params, name: Optional[str], cls_name: str,
                 j_op: Optional[JavaObject] = None, model: Optional[JavaObjectWrapper] = None):
        """
        Construct a Java object, then set its parameters from `params`.
        Following ways of constructors are supported:

        #. if `j_op` is not `None`, directly wrap it;
        #. if `j_op` is `None` and `model` is `None`, construct a Java object with empty constructor;
        #. if `j_op` is `None` and `model` is not `None`, construct a Java object with exact 1 parameter `model.get_j_obj()`.

        The last constructor is usually for model predict operators of stream type.

        :param params: parameters.
        :param name: name.
        :param cls_name: name of corresponding Java class.
        :param j_op: a Java object.
        :param model: model operator.
        """
        super().__init__(params)

        AlgoOperator._inner_counter += 1
        self.idx = AlgoOperator._inner_counter
        self.name = name

        if j_op is not None:
            self._j_op = j_op
        else:
            if model is None:
                self._j_op = get_java_class(cls_name)()
            else:
                self._j_op = get_java_class(cls_name)(model.get_j_obj())

        self.params = Params.from_args(params)
        for k, v in self.params.items():
            self._add_param(k, v)
        self.inputs = []

    def get_j_obj(self) -> JavaObject:
        return self._j_op

    def getName(self) -> str:
        """Get the distinct name. If `name` is `None` when constructing, automatically generate one."""
        if self.name is None:
            return self.__class__.__name__ + '-' + str(self.idx)
        return self.name

    @abstractmethod
    def getSideOutput(self, index) -> 'AlgoOperator':
        """
        Get side output with chosen index.

        :param index: index
        :return: return the chosen side output.
        """
        ...

    def getSideOutputCount(self) -> int:
        """
        Get the count of side outputs.
        
        :return:the count of side outputs.
        """
        return self.get_j_obj().getSideOutputCount()

    def getOutputTable(self) -> 'Table':
        """
        Get the output table represented by this operator.

        :return: the output table.
        """
        # noinspection PyProtectedMember
        from ....env import _mlenv
        _, btenv, _, stenv = _mlenv
        tenv = self._choose_by_op_type(btenv, stenv)
        return Table(self.get_j_obj().getOutputTable(), tenv)

    @abstractmethod
    def linkFrom(self, *ops: 'AlgoOperator') -> 'AlgoOperator':
        """
        Link from `ops` to `self`, i.e. use outputs of `ops` as input of `self`.

        :param ops: the operators to link to.
        :return: `self`.
        """
        ...

    def linkTo(self, op: 'AlgoOperator') -> 'AlgoOperator':
        """
        Link from `self` to `op`.

        :param op: the operator.
        :return: `op`.
        """
        op.linkFrom(self)
        return op

    def link(self, op: 'AlgoOperator') -> 'AlgoOperator':
        """
        Link from `self` to `op`.

        :param op: the operator.
        :return: `op`.
        """
        return self.linkTo(op)

    def _choose_by_op_type(self, batch_choice, stream_choice):
        from ....batch.base import BatchOperator
        from ....stream.base import StreamOperator
        if isinstance(self, BatchOperator):
            return batch_choice
        if isinstance(self, StreamOperator):
            return stream_choice
        raise Exception("op %s should be BatchOperator or StreamOperator" % self)

    def udf(self, func, selectedCols: List[str], outputCol: str, reservedCols: List[str] = None):
        """
        Link `self` to a :py:class:`UDFBatchOp` or :py:class:`UDFStreamOp` with provided arguments based on the type of `self`.

        :param func: UDF function.
        :param selectedCols: selected column names.
        :param outputCol: output column name.
        :param reservedCols: reserved column names.
        :return: return a :py:class:`UDFBatchOp` or :py:class:`UDFStreamOp`.
        """
        from ....batch.special_operators import UDFBatchOp
        from ....stream.special_operators import UDFStreamOp
        udf_op_cls = self._choose_by_op_type(UDFBatchOp, UDFStreamOp)
        udf_op = udf_op_cls() \
            .setFunc(func) \
            .setSelectedCols(selectedCols) \
            .setOutputCol(outputCol)
        if reservedCols is not None:
            udf_op = udf_op.setReservedCols(reservedCols)
        return self.link(udf_op)

    def udtf(self, func, selectedCols: List[str], outputCols: List[str], reservedCols: List[str] = None):
        """
        Link `self` to a :py:class:`UDTFBatchOp` or :py:class:`UDTFStreamOp` with provided arguments based on the type of `self`.

        :param func: UDF function.
        :param selectedCols: selected column names.
        :param outputCols: output column names.
        :param reservedCols: reserved column names.
        :return: return a :py:class:`UDTFBatchOp` or :py:class:`UDTFStreamOp`.
        """
        from ....batch.special_operators import UDTFBatchOp
        from ....stream.special_operators import UDTFStreamOp
        udtf_op_cls = self._choose_by_op_type(UDTFBatchOp, UDTFStreamOp)
        udtf_op = udtf_op_cls() \
            .setFunc(func) \
            .setSelectedCols(selectedCols) \
            .setOutputCols(outputCols)
        if reservedCols is not None:
            udtf_op = udtf_op.setReservedCols(reservedCols)
        return self.link(udtf_op)

    def select(self, fields: Union[str, list, tuple]):
        """
        Link `self` to a :py:class:`SelectBatchOp` or :py:class:`SelectStreamOp` with provided arguments based on the type of `self`.

        :param fields: select clause or multiple fields.
        :return: a :py:class:`SelectBatchOp` or :py:class:`SelectStreamOp`.
        """
        if isinstance(fields, (list, tuple)):
            clause = ",".join(map(lambda d: "`" + d + "`", fields))
        else:
            clause = fields
        from ....batch.common import SelectBatchOp
        from ....stream.common import SelectStreamOp
        select_op_cls = self._choose_by_op_type(SelectBatchOp, SelectStreamOp)
        return self.link(select_op_cls().setClause(clause))

    def alias(self, fields: Union[str, list, tuple]):
        """
        Link `self` to a :py:class:`AsBatchOp` or :py:class:`AsStreamOp` with provided arguments based on the type of `self`.

        :param fields: select clause or multiple fields.
        :return: a :py:class:`AsBatchOp` or :py:class:`AsStreamOp`.
        """
        if isinstance(fields, (list, tuple)):
            clause = ",".join(fields)
        else:
            clause = fields
        from ....batch.common import AsBatchOp
        from ....stream.common import AsStreamOp
        as_op_cls = self._choose_by_op_type(AsBatchOp, AsStreamOp)
        return self.link(as_op_cls().setClause(clause))

    def where(self, predicate: str):
        """
        Link `self` to a :py:class:`WhereBatchOp` or :py:class:`WhereStreamOp` with provided arguments based on the type of `self`.

        :param predicate: predicate for where.
        :return: a :py:class:`WhereBatchOp` or :py:class:`WhereStreamOp`.
        """
        from ....batch.common import WhereBatchOp
        from ....stream.common import WhereStreamOp
        where_op_cls = self._choose_by_op_type(WhereBatchOp, WhereStreamOp)
        return self.link(where_op_cls().setClause(predicate))

    def filter(self, predicate: str):
        """
        Link `self` to a :py:class:`FilterBatchOp` or :py:class:`FilterStreamOp` with provided arguments based on the type of `self`.

        :param predicate: predicate for filter.
        :return: a :py:class:`FilterBatchOp` or :py:class:`FilterStreamOp`.
        """
        from ....batch.common import FilterBatchOp
        from ....stream.common import FilterStreamOp
        filter_op_cls = self._choose_by_op_type(FilterBatchOp, FilterStreamOp)
        return self.link(filter_op_cls().setClause(predicate))

    def getColNames(self) -> List[str]:
        """
        Get column names represented by`self` if `self` is already linked from others.
        Otherwise, an exception is raised.

        :return: column names.
        """
        return list(self._j_op.getColNames())

    def getColTypes(self) -> List[str]:
        """
        Get column types represented by`self` if `self` is already linked from others.
        Otherwise, an exception is raised.

        :return: column types.
        """
        FlinkTypeConverter = get_java_class("com.alibaba.alink.operator.common.io.types.FlinkTypeConverter")
        coltypes = self._j_op.getColTypes()
        return [str(FlinkTypeConverter.getTypeString(i)) for i in coltypes]

    def getSchemaStr(self) -> str:
        """
        Get schema string represented by`self` if `self` is already linked from others.
        Otherwise, an exception is raised.

        :return: schema string.
        """
        col_names = self.getColNames()
        col_types = self.getColTypes()
        return ", ".join([k + " " + v for k, v in zip(col_names, col_types)])
