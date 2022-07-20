from abc import ABC

import pandas
from py4j.java_gateway import JavaObject

from .display_utils import DataStreamCollectorThread
from ..common.sql.sql_query_utils import register_table_name, sql_query
from ..common.types.bases.algo_operator import AlgoOperator
from ..common.types.bases.params import Params
from ..common.types.conversion.type_converters import py_list_to_j_array, dataframeToOperator
from ..py4j_util import get_java_class

__all__ = ['StreamOperator', 'StreamOperatorWrapper', 'BaseSourceStreamOp', 'BaseSinkStreamOp', 'BaseModelStreamOp']


class StreamOperator(AlgoOperator):
    """
    The base class of all :py:class:`StreamOperator` s.
    Its instance wraps a Java object of type `StreamOperator`.
    """

    _print_ops = []

    def __init__(self, j_op=None, model=None, *args, **kwargs):
        """
        Construct a Java object, then set its parameters with a :py:class:`Params` instance.

        The :py:class:`Params` instance is constructed from `args` and `kwargs` using :py:func:`Params.from_args`.

        Following ways of constructors are supported:

        #. if `j_op` is not `None`, directly wrap it;
        #. if `j_op` is `None`, construct a Java object of class `cls_name` with empty constructor (`cls_name` is obtained from `kwargs` with key `CLS_NAME`);
        #. if `j_op` is `None` and `model` is not `None`, construct a Java object with exact 1 parameter `model.get_j_obj()`.


        `name` and `OP_TYPE` are optionally extracted from `kwargs` with key `name` and `OP_TYPE` respectively.

        :param j_op: a Java `StreamOperator` object.
        :param model: model operator.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        name = kwargs.pop('name', None)
        cls_name = kwargs.pop('CLS_NAME', None)
        self.opType = kwargs.pop('OP_TYPE', 'FUNCTION')
        params = Params.from_args(*args, **kwargs)
        super(StreamOperator, self).__init__(params, name, cls_name, j_op=j_op, model=model)

    def linkFrom(self, *ops):
        j_stream_operator_class = get_java_class("com.alibaba.alink.operator.stream.StreamOperator")
        if len(ops) == 1 and isinstance(ops[0], (list, tuple)):
            ops = ops[0]
        num = len(ops)
        j_ops = list(map(lambda op: op.get_j_obj(), ops))
        j_args = py_list_to_j_array(j_stream_operator_class, num, j_ops)
        self._j_op.linkFrom(j_args)
        super(StreamOperator, self).linkFrom(ops)
        return self

    @classmethod
    def execute(cls, jobName: str = None):
        """
        Trigger the program execution.

        The environment will execute all parts of the program that have resulted in a "sink" operation.
        Sink operations include :py:func:`StreamOperator.print`, or data sink functions.
        An exception is thrown if no sink operators found.

        :param jobName: optional, job name for this execution.
        """
        for op in StreamOperator._print_ops:
            op.collector_thread.daemon = True
            op.collector_thread.start()
        j_stream_operator_cls = get_java_class("com.alibaba.alink.operator.stream.StreamOperator")
        try:
            if jobName is None:
                j_stream_operator_cls.execute()
            else:
                j_stream_operator_cls.execute(jobName)
        finally:
            for op in StreamOperator._print_ops:
                op.collector_thread.stop()
            # for op in StreamOperator._print_ops:
            #     op.collector_thread.join()
            StreamOperator._print_ops = []

    def print(self, key=None, refreshInterval=0, maxLimit=100, port=0):
        """
        Register print function for this operator.
        Contents are printed only after the program execution is triggered by :py:func:`StreamOperator.execute`.

        To receive data from Flink workers, network access from client side (the machine to run PyAlink script) to Flink workers is required.
        A random port is opened for receiving data by default, or specified by the parameter `port`.

        If called in IPython environment, like Jupyter Notebook, the content being printed will be refreshed in every `refreshInterval` seconds, with a limit of `maxLimit`.
        Otherwise, content is displayed in a streaming fashion.

        :param key: an identifier for the call of `print`, reserved for future use.
        :param refreshInterval: refreshed interval of display, valid in IPython environment.
        :param maxLimit: max limit displayed in every display interval, valid in IPython environment.
        :param port: the port to receive data from Flink workers.
        """
        if key is None:
            import uuid
            key = uuid.uuid4().hex
        self._print_ipython(key, refreshInterval, maxLimit, port)

    def _print_ipython(self, key, refresh_interval, max_limit, port):
        self.collector_thread = DataStreamCollectorThread(self, key, refresh_interval, max_limit, port)
        StreamOperator._print_ops.append(self)

    @staticmethod
    def fromDataframe(df: pandas.DataFrame, schemaStr: str) -> 'StreamOperator':
        """
        Construct a :py:class:`StreamOperator` instance from a :py:class:`pandas.DataFrame` instance.

        :param df: the :py:class:`pandas.DataFrame` instance.
        :param schemaStr: schema string.
        :return: a :py:class:`StreamOperator` instance.
        """
        return dataframeToOperator(df, schemaStr, op_type="stream")

    def getSideOutput(self, index):
        from .common import SideOutputStreamOp
        return SideOutputStreamOp().setIndex(index).linkFrom(self)

    def sample(self, ratio):
        """
        Link `self` to a :py:class:`SampleStreamOp` with passed arguments.

        :param ratio: sampling ratio.
        :return: a :py:class:`SampleStreamOp` instance.
        """
        from .common import SampleStreamOp
        return self.link(SampleStreamOp().setRatio(ratio))

    def registerTableName(self, name):
        """
        Register `self` as a table in the stream environment, which can be later used in SQL.

        :param name: table name.
        """
        self.get_j_obj().registerTableName(name)
        register_table_name(self, name, "stream")

    @staticmethod
    def sqlQuery(query):
        """
        Returns a :py:class:`StreamOperator` representing the result after processing SQL query.

        :param query: SQL query.
        :return: a :py:class:`StreamOperator` representing the result after processing SQL query.
        """
        return sql_query(query, "stream")

    @staticmethod
    def registerFunction(name, func):
        """
        Register a function in the stream environment, which can be later used as a UDf or UDTF in SQL query.

        :param name: name of the function.
        :param func: function.
        """
        from ..udf.utils import register_pyflink_function
        register_pyflink_function(name, func, 'stream')


class StreamOperatorWrapper(StreamOperator):
    """
    Wrap a Java object to a :py:class:`StreamOperator` instance.
    """

    def __init__(self, j_op: JavaObject):
        """
        Wrap a Java object of type `StreamOperator` to a :py:class:`StreamOperator` instance.

        :param j_op: the Java object.
        """
        super(StreamOperatorWrapper, self).__init__(j_op=j_op)


class BaseSourceStreamOp(StreamOperator, ABC):
    """
    Base class for source stream operators.
    """

    def __init__(self, *args, **kwargs):
        """"""
        kwargs['OP_TYPE'] = 'SOURCE'
        super(BaseSourceStreamOp, self).__init__(*args, **kwargs)

    def linkFrom(self, *args):
        raise RuntimeError('Source operator does not support linkFrom()')


class BaseSinkStreamOp(StreamOperator, ABC):
    """
    Base class for sink stream operators.
    """

    def __init__(self, *args, **kwargs):
        """"""
        kwargs['OP_TYPE'] = 'SINK'
        super(BaseSinkStreamOp, self).__init__(*args, **kwargs)


class BaseModelStreamOp(StreamOperator, ABC):
    """
    Base class for model stream operators.
    """

    def __init__(self, *args, **kwargs):
        """"""
        if "model" in kwargs:
            self.model = kwargs.pop("model")
        elif len(args) >= 1:
            self.model = args[0]
            args = args[1:]
        else:
            raise Exception("A BatchOperator representing model must be provided.")
        super(BaseModelStreamOp, self).__init__(j_op=None, model=self.model, *args, **kwargs, )

    def linkFrom(self, *args):
        super(BaseModelStreamOp, self).linkFrom(*args)
        self.inputs = [self.model] + [x for x in args]
        return self
