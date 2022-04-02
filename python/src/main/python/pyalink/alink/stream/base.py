from .display_utils import DataStreamCollectorThread
from ..common.sql.sql_query_utils import register_table_name, sql_query
from ..common.types.bases.algo_operator import AlgoOperator
from ..common.types.bases.params import Params
from ..common.types.conversion.type_converters import py_list_to_j_array, dataframeToOperator
from ..py4j_util import get_java_class


class StreamOperator(AlgoOperator):
    _print_ops = []

    def __init__(self, j_op=None, model=None, *args, **kwargs):
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
    def execute(cls):
        for op in StreamOperator._print_ops:
            op.collector_thread.daemon = True
            op.collector_thread.start()
        j_stream_operator_cls = get_java_class("com.alibaba.alink.operator.stream.StreamOperator")
        try:
            j_stream_operator_cls.execute()
        finally:
            for op in StreamOperator._print_ops:
                op.collector_thread.stop()
            # for op in StreamOperator._print_ops:
            #     op.collector_thread.join()
            StreamOperator._print_ops = []

    def print(self, key=None, refreshInterval=0, maxLimit=100, port=0):
        if key is None:
            import uuid
            key = uuid.uuid4().hex
        self._print_ipython(key, refreshInterval, maxLimit, port)

    def _print_ipython(self, key, refresh_interval, max_limit, port):
        self.collector_thread = DataStreamCollectorThread(self, key, refresh_interval, max_limit, port)
        StreamOperator._print_ops.append(self)

    @staticmethod
    def fromDataframe(df, schemaStr):
        return dataframeToOperator(df, schemaStr, opType="stream")

    def getSideOutput(self, index):
        from .common import SideOutputStreamOp
        return SideOutputStreamOp().setIndex(index).linkFrom(self)

    def sample(self, ratio):
        from .common import SampleStreamOp
        return self.link(SampleStreamOp().setRatio(ratio))

    def registerTableName(self, name):
        self.get_j_obj().registerTableName(name)
        register_table_name(self, name, "stream")

    @staticmethod
    def sqlQuery(query):
        return sql_query(query, "stream")

    @staticmethod
    def registerFunction(name, func):
        from ..udf.utils import register_pyflink_function
        register_pyflink_function(name, func, 'stream')


class StreamOperatorWrapper(StreamOperator):
    def __init__(self, op):
        self.op = op
        super(StreamOperatorWrapper, self).__init__(j_op=op)


class BaseSourceStreamOp(StreamOperator):
    def __init__(self, j_op=None, *args, **kwargs):
        kwargs['OP_TYPE'] = 'SOURCE'
        super(BaseSourceStreamOp, self).__init__(j_op=j_op, *args, **kwargs)

    def linkFrom(self, *args):
        raise RuntimeError('Source operator does not support linkFrom()')


class BaseSinkStreamOp(StreamOperator):
    def __init__(self, *args, **kwargs):
        kwargs['OP_TYPE'] = 'SINK'
        super(BaseSinkStreamOp, self).__init__(*args, **kwargs)


class BaseModelStreamOp(StreamOperator):
    def __init__(self, *args, **kwargs):
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
