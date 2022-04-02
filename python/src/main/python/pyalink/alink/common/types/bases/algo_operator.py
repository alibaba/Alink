from abc import abstractmethod

from .params import Params
from .with_params import WithParams
from ....py4j_util import get_java_class

__all__ = ['AlgoOperator']


class AlgoOperator(WithParams):
    _inner_counter = 0

    def get_j_obj(self):
        return self._j_op

    def __init__(self, params, name, cls_name, j_op=None, model=None):
        super().__init__(params)

        AlgoOperator._inner_counter += 1
        self.idx = AlgoOperator._inner_counter
        self.name = name
        self.clsName = cls_name

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

    def getName(self):
        if self.name is None:
            return self.__class__.__name__ + '-' + str(self.idx)
        return self.name

    @abstractmethod
    def getSideOutput(self, index):
        ...

    def getSideOutputCount(self):
        return self.get_j_obj().getSideOutputCount()

    def getOutputTable(self):
        from pyflink.table import Table
        # noinspection PyProtectedMember
        from ....env import _mlenv
        _, btenv, _, stenv = _mlenv
        tenv = self._choose_by_op_type(btenv, stenv)
        return Table(self.get_j_obj().getOutputTable(), tenv)

    @abstractmethod
    def linkFrom(self, *args):
        ...

    def linkTo(self, op):
        op.linkFrom(self)
        return op

    def link(self, op):
        return self.linkTo(op)

    def _choose_by_op_type(self, batch_choice, stream_choice):
        from ....batch.base import BatchOperator
        from ....stream.base import StreamOperator
        if isinstance(self, BatchOperator):
            return batch_choice
        if isinstance(self, StreamOperator):
            return stream_choice
        raise Exception("op %s should be BatchOperator or StreamOperator" % self)

    def udf(self, func, selectedCols, outputCol, reservedCols=None):
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

    def udtf(self, func, selectedCols, outputCols, reservedCols=None):
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

    def select(self, fields):
        if isinstance(fields, (list, tuple)):
            clause = ",".join(map(lambda d: "`" + d + "`", fields))
        else:
            clause = fields
        from ....batch.common import SelectBatchOp
        from ....stream.common import SelectStreamOp
        select_op_cls = self._choose_by_op_type(SelectBatchOp, SelectStreamOp)
        return self.link(select_op_cls().setClause(clause))

    def alias(self, fields):
        if isinstance(fields, (list, tuple)):
            clause = ",".join(fields)
        else:
            clause = fields
        from ....batch.common import AsBatchOp
        from ....stream.common import AsStreamOp
        as_op_cls = self._choose_by_op_type(AsBatchOp, AsStreamOp)
        return self.link(as_op_cls().setClause(clause))

    def where(self, predicate):
        from ....batch.common import WhereBatchOp
        from ....stream.common import WhereStreamOp
        where_op_cls = self._choose_by_op_type(WhereBatchOp, WhereStreamOp)
        return self.link(where_op_cls().setClause(predicate))

    def filter(self, predicate):
        from ....batch.common import FilterBatchOp
        from ....stream.common import FilterStreamOp
        filter_op_cls = self._choose_by_op_type(FilterBatchOp, FilterStreamOp)
        return self.link(filter_op_cls().setClause(predicate))

    def getColNames(self):
        return list(self._j_op.getColNames())

    def getColTypes(self):
        FlinkTypeConverter = get_java_class("com.alibaba.alink.operator.common.io.types.FlinkTypeConverter")
        coltypes = self._j_op.getColTypes()
        return [str(FlinkTypeConverter.getTypeString(i)) for i in coltypes]

    def getSchemaStr(self):
        col_names = self.getColNames()
        col_types = self.getColTypes()
        return ", ".join([k + " " + v for k, v in zip(col_names, col_types)])
