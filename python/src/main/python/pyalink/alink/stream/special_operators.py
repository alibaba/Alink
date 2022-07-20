from typing import Union

from .common import FtrlPredictStreamOp as _FtrlPredictStreamOp
from .common import FtrlTrainStreamOp as _FtrlTrainStreamOp
from .common import OnlineLearningStreamOp as _OnlineLearningStreamOp
from .common import PipelinePredictStreamOp as _PipelinePredictStreamOp
from .common import PyScalarFnStreamOp as _PyScalarFnStreamOp
from .common import PyTableFnStreamOp as _PyTableFnStreamOp
from ..batch.base import BatchOperator
from ..py4j_util import get_java_class
from ..stream import StreamOperator
from ..udf.utils import do_set_op_udf, do_set_op_udtf

__all__ = ['UDFStreamOp', 'UDTFStreamOp', 'FtrlTrainStreamOp', 'FtrlPredictStreamOp', 'TableSourceStreamOp',
           'PipelinePredictStreamOp', 'OnlineLearningStreamOp']


class UDFStreamOp(_PyScalarFnStreamOp):
    """
    Similar as `UDFStreamOp` in Java side, except for supporting Python udf other than Java udf.
    """

    def __init__(self, *args, **kwargs):
        """"""
        super(UDFStreamOp, self).__init__(*args, **kwargs)

    def setFunc(self, func):
        """
        Set UDF function

        :param func: an instance with `eval` attribute, or `callable`, or an instance of :py:class:`UserDefinedScalarFunctionWrapper`.
        :return: `self`.
        """
        return do_set_op_udf(self, func)


class UDTFStreamOp(_PyTableFnStreamOp):
    """
    Similar as `UDTFStreamOp` in Java side, except for supporting Python udtf other than Java udtf.
    """

    def __init__(self, *args, **kwargs):
        """"""
        super(UDTFStreamOp, self).__init__(*args, **kwargs)

    def setFunc(self, func):
        """
        Set UDTF function

        :param func: an instance with `eval` attribute, or `callable`, or an instance of :py:class:`UserDefinedTableFunctionWrapper`.
        :return: `self`.
        """
        return do_set_op_udtf(self, func)


class FtrlTrainStreamOp(_FtrlTrainStreamOp):
    def __init__(self, model, *args, **kwargs):
        self.model = model
        super(FtrlTrainStreamOp, self).__init__(model=model, *args, **kwargs)

    def linkFrom(self, *args):
        self.inputs = [self.model] + [x for x in args]
        return super(FtrlTrainStreamOp, self).linkFrom(*args)


class FtrlPredictStreamOp(_FtrlPredictStreamOp):
    def __init__(self, model, *args, **kwargs):
        self.model = model
        super(FtrlPredictStreamOp, self).__init__(model=model, *args, **kwargs)

    def linkFrom(self, *args):
        self.inputs = [self.model] + [x for x in args]
        return super(FtrlPredictStreamOp, self).linkFrom(*args)


class TableSourceStreamOp(StreamOperator):
    """
    Convert a PyFlink :py:class:`Table` to a :py:class:`StreamOperator`.
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
        table_source_stream_op_cls = get_java_class("com.alibaba.alink.operator.stream.source.TableSourceStreamOp")
        # noinspection PyProtectedMember
        j_op = table_source_stream_op_cls(table._j_table)
        super(TableSourceStreamOp, self).__init__(j_op=j_op, *args, **kwargs)


class PipelinePredictStreamOp(_PipelinePredictStreamOp):
    def __init__(self, pipeline_model_or_path: Union['PipelineModel', str], *args, **kwargs):
        from ..pipeline.base import PipelineModel
        if isinstance(pipeline_model_or_path, (PipelineModel,)):
            model = pipeline_model_or_path
        else:
            model = PipelineModel.load(pipeline_model_or_path)
        super(PipelinePredictStreamOp, self).__init__(model=model, *args, **kwargs)


class OnlineLearningStreamOp(_OnlineLearningStreamOp):
    def __init__(self, pipeline_model_or_batch_op: Union['PipelineModel', BatchOperator], *args, **kwargs):
        from ..pipeline.base import PipelineModel
        if isinstance(pipeline_model_or_batch_op, (PipelineModel,)):
            model = pipeline_model_or_batch_op.save()
        else:
            model = pipeline_model_or_batch_op
        super(OnlineLearningStreamOp, self).__init__(model=model, *args, **kwargs)
