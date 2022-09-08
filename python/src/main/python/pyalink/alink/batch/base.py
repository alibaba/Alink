from abc import ABC
from typing import Optional, Callable

import pandas
from py4j.java_gateway import JavaObject

from .lazy_evaluation import pipe_j_lazy_to_py_callbacks
from ..common.sql.sql_query_utils import register_table_name, sql_query
from ..common.types.bases.algo_operator import AlgoOperator
from ..common.types.bases.params import Params
from ..common.types.conversion.java_method_call import auto_convert_java_type, call_java_method
from ..common.types.conversion.type_converters import py_list_to_j_array, dataframeToOperator, \
    lazy_collect_to_dataframes, collectToDataframes, j_value_to_py_value
from ..common.utils.printing import print_with_title
from ..py4j_util import get_java_class
from ..udf.utils import register_pyflink_function

__all__ = ['BatchOperator', 'BaseSourceBatchOp', 'BaseSinkBatchOp', 'BatchOperatorWrapper']


class BatchOperator(AlgoOperator):
    """
    The base class of all :py:class:`BatchOperator` s.
    Its instance wraps a Java object of type `BatchOperator`.
    """

    def __init__(self, j_op: Optional[JavaObject] = None, *args, **kwargs):
        """
        Construct a Java object, then set its parameters with a :py:class:`Params` instance.

        The :py:class:`Params` instance is constructed from `args` and `kwargs` using :py:func:`Params.from_args`.

        Following ways of constructors are supported:

        #. if `j_op` is not `None`, directly wrap it;
        #. if `j_op` is `None`, construct a Java object of class `cls_name` with empty constructor (`cls_name` is obtained from `kwargs` with key `CLS_NAME`);

        `name` and `OP_TYPE` are optionally extracted from `kwargs` with key `name` and `OP_TYPE` respectively.

        :param j_op: a Java `BatchOperator` object.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        name = kwargs.pop('name', None)
        cls_name = kwargs.pop('CLS_NAME', None)
        self.opType = kwargs.pop('OP_TYPE', 'FUNCTION')
        params = Params.from_args(*args, **kwargs)
        super(BatchOperator, self).__init__(params, name, cls_name, j_op)

    def linkFrom(self, *ops):
        j_batch_operator_class = get_java_class("com.alibaba.alink.operator.batch.BatchOperator")
        if len(ops) == 1 and isinstance(ops[0], (list, tuple)):
            ops = ops[0]
        num = len(ops)
        j_ops = list(map(lambda op: op.get_j_obj(), ops))
        j_args = py_list_to_j_array(j_batch_operator_class, num, j_ops)
        self.get_j_obj().linkFrom(j_args)
        super(BatchOperator, self).linkFrom(ops)
        return self

    @classmethod
    def execute(cls, jobName: str = None):
        """
        Trigger the program execution.

        The environment will execute all parts of the program that have resulted in a "sink" operation.
        Sink operations include :py:func:`BatchOperator.print`, :py:func:`BatchOperator.collectToDataframe`, lazy print or collect functions.
        An exception is thrown if no sink operators found.

        :param jobName: optional, job name for this execution.
        """
        j_batch_operator_class = get_java_class("com.alibaba.alink.operator.batch.BatchOperator")
        if jobName is None:
            j_batch_operator_class.execute()
        else:
            j_batch_operator_class.execute(jobName)

    def collectToDataframe(self) -> pandas.DataFrame:
        """
        Trigger the program execution like :py:func:`BatchOperator.execute`, and
        collect the data in this operator to a :py:class:`pandas.DataFrame` instance.

        :return: the :py:class:`pandas.DataFrame` instance.
        """
        return collectToDataframes(self)[0]

    @staticmethod
    def fromDataframe(df: pandas.DataFrame, schemaStr: str) -> 'BatchOperator':
        """
        Construct a :py:class:`BatchOperator` instance from a :py:class:`pandas.DataFrame` instance.

        :param df: the :py:class:`pandas.DataFrame` instance.
        :param schemaStr: schema string.
        :return: a :py:class:`BatchOperator` instance.
        """
        return dataframeToOperator(df, schemaStr, op_type="batch")

    def print(self, n: int = 0, title: str = None):
        """
        Trigger the program execution like :py:func:`BatchOperator.execute`, and
        print data in this operator as a :py:class:`pandas.DataFrame` instance.

        :param n: number of records to print, a negative number or 0 means using default print behaviors for dataframes, e.g. ellipsis may be used when a large number of records in the data.
        :param title: title to be prepended before data.
        :return: `self`.
        """
        self.lazyPrint(n, title)
        BatchOperator.execute()

    def getSideOutput(self, index):
        from .common import SideOutputBatchOp
        return SideOutputBatchOp().setIndex(index).linkFrom(self)

    def firstN(self, n: int):
        """
        Link `self` to a :py:class:`FirstNBatchOp` with passed arguments.

        :param n: number of records.
        :return: a :py:class:`FirstNBatchOp` instance.
        """
        from .common import FirstNBatchOp
        return self.linkTo(FirstNBatchOp().setSize(n))

    def sample(self, ratio: float, withReplacement=False):
        """
        Link `self` to a :py:class:`SampleBatchOp` with passed arguments.

        :param ratio: sampling ratio.
        :param withReplacement: with replacement or not.
        :return: a :py:class:`SampleBatchOp` instance.
        """
        from .common import SampleBatchOp
        return self.link(SampleBatchOp().setRatio(ratio).setWithReplacement(withReplacement))

    def sampleWithSize(self, numSamples, withReplacement=False):
        """
        Link `self` to a :py:class:`SampleWithSizeBatchOp` with passed arguments.

        :param numSamples: sampling number.
        :param withReplacement: with replacement or not.
        :return: a :py:class:`SampleWithSizeBatchOp` instance.
        """
        from .common import SampleWithSizeBatchOp
        return self.link(SampleWithSizeBatchOp().setSize(numSamples).setWithReplacement(withReplacement))

    def distinct(self):
        """
        Link `self` to a :py:class:`DistinctBatchOp` with passed arguments.

        :return: a :py:class:`DistinctBatchOp` instance.
        """
        from .common import DistinctBatchOp
        return self.link(DistinctBatchOp())

    def orderBy(self, field: str, limit: int = -1, fetch: int = -1, offset: int = -1, order="asc"):
        """
        Link `self` to a :py:class:`OrderByBatchOp` with passed arguments.

        :param field: clause of order by.
        :type limit: limit.
        :param fetch: fetch size, use with `offset`.
        :param offset: offset, use with `fetch`.
        :param order: sort order: 'asc' or 'desc'.

        :return: a :py:class:`OrderByBatchOp` instance.
        """
        from .common import OrderByBatchOp
        order_by_op = OrderByBatchOp() \
            .setClause(field) \
            .setOrder(order)
        if limit > 0:
            order_by_op = order_by_op.setLimit(limit)
        elif fetch > 0 and offset > 0:
            order_by_op = order_by_op.setFetch(fetch) \
                .setOffset(offset)
        return self.link(order_by_op)

    def groupBy(self, by: str, select: str):
        """
        Link `self` to a :py:class:`GroupByBatchOp` with passed arguments.

        :param by: the field used for group.
        :param select: select clause.

        :return: a :py:class:`GroupByBatchOp` instance.
        """
        from .common import GroupByBatchOp
        group_by_batch_op = GroupByBatchOp() \
            .setGroupByPredicate(by) \
            .setSelectClause(select)
        return self.link(group_by_batch_op)

    def rebalance(self):
        """
        Link `self` to a :py:class:`RebalanceBatchOp` with passed arguments.

        :return: a :py:class:`RebalanceBatchOp` instance.
        """
        from .common import RebalanceBatchOp
        return self.link(RebalanceBatchOp())

    def shuffle(self):
        """
        Link `self` to a :py:class:`ShuffleBatchOp` with passed arguments.

        :return: a :py:class:`ShuffleBatchOp` instance.
        """
        from .common import ShuffleBatchOp
        return self.link(ShuffleBatchOp())

    def registerTableName(self, name: str):
        """
        Register `self` as a table in the batch environment, which can be later used in SQL.

        :param name: table name.
        """
        self.get_j_obj().registerTableName(name)
        register_table_name(self, name, "batch")

    @staticmethod
    def sqlQuery(query: str) -> 'BatchOperator':
        """
        Returns a :py:class:`BatchOperator` representing the result after processing SQL query.

        :param query: SQL query.
        :return: a :py:class:`BatchOperator` representing the result after processing SQL query.
        """
        return sql_query(query, "batch")

    @staticmethod
    def registerFunction(name: str, func):
        """
        Register a function in the batch environment, which can be later used as a UDf or UDTF in SQL query.

        :param name: name of the function.
        :param func: function.
        """
        register_pyflink_function(name, func, 'batch')

    def lazyPrint(self, n: int = 0, title: str = None):
        """
        Lazily print data in this operator as a :py:class:`pandas.DataFrame`, e.g. the print action is performed when next execution triggered.

        :param n: number of records to print, a negative number or 0 means using default print behaviors for dataframes, e.g. ellipsis may be used when a large number of records in the data.
        :param title: title prepended to the data.
        :return: `self`.
        """
        from .common import FirstNBatchOp
        if n > 0:
            op = self.linkTo(FirstNBatchOp().setSize(n))
            op.lazyCollectToDataframe(lambda df: print_with_title(df, title, print_all=True))
        else:
            self.lazyCollectToDataframe(lambda df: print_with_title(df, title))
        return self

    def lazyCollectToDataframe(self, *callbacks: Callable[[pandas.DataFrame], None]):
        """
        Lazily collect data in this operator to a :py:class:`pandas.DataFrame`, and call `callbacks`.

        :param callbacks: callback functions applied to :py:class:`pandas.DataFrame`.
        :return: `self`.
        """
        lazy_dataframe = lazy_collect_to_dataframes(self)[0]
        for callback in callbacks:
            lazy_dataframe.addCallback(callback)
        return self

    @auto_convert_java_type
    def collectStatistics(self):
        """
        Trigger the program execution like :py:func:`BatchOperator.execute`, and
        collect statistics of data in this operator.

        :return: a :py:class:`TableSummary` instance representing statistics.
        """
        return self.collectStatistics()

    def lazyCollectStatistics(self, *callbacks):
        """
        Lazily collect statistics of data in this operator, and call `callbacks`.

        :param callbacks: callback functions applied to :py:class:`TableSummary`.
        :return: `self`
        """
        pipe_j_lazy_to_py_callbacks(
            self.get_j_obj().lazyCollectStatistics,
            callbacks,
            j_value_to_py_value)
        return self

    def lazyPrintStatistics(self, title: str = None):
        """
        Lazily print statistics of data in this operator.

        :param title: title prepended the statistics.
        :return: `self`
        """
        return self.lazyCollectStatistics(lambda summary: print_with_title(summary, title))

    def lazyVizStatistics(self, tableName: str = None):
        from .operator_with_methods import InternalFullStatsBatchOp
        internalFullStatsBatchOp = InternalFullStatsBatchOp().linkFrom(self)
        new_table_names = [tableName] if tableName is not None else []
        internalFullStatsBatchOp.lazyVizFullStats(new_table_names)
        return self

    def lazyVizDive(self):
        def console_callback(df: pandas.DataFrame):
            s = df.to_json(orient='records')
            j_dive_visualizer = get_java_class("com.alibaba.alink.operator.batch.utils.DiveVisualizer").getInstance()
            call_java_method(j_dive_visualizer.visualize, s)

        def ipython_callback(df: pandas.DataFrame):
            s = df.to_json(orient='records')
            j_dive_visualizer = get_java_class("com.alibaba.alink.operator.batch.utils.DiveVisualizer").getInstance()
            srcdoc = call_java_method(j_dive_visualizer.generateIframeHtml, s)
            from html import escape
            html = """\
            <iframe srcdoc='{srcdoc}' id='facets-dive-iframe' width="100%" height="500px"></iframe>
            <script>
                facets_dive_iframe = document.getElementById('facets-dive-iframe');
                facets_dive_iframe.id = "";
                setTimeout(() => {
                    facets_dive_iframe.setAttribute('height', facets_dive_iframe.contentWindow.document.body.offsetHeight + 'px')
                }, 1500)
            </script>
            """.replace('{srcdoc}', escape(srcdoc))

            from IPython.display import display, HTML
            display(HTML(html))

        from ..common.utils.packages import in_ipython
        callback = ipython_callback if in_ipython() else console_callback
        self.sampleWithSize(10000).lazyCollectToDataframe(callback)
        return self


class BaseSourceBatchOp(BatchOperator, ABC):
    """
    Base class for source batch operators.
    """

    def __init__(self, *args, **kwargs):
        """"""
        kwargs['OP_TYPE'] = 'SOURCE'
        super(BaseSourceBatchOp, self).__init__(*args, **kwargs)

    def linkFrom(self, *args):
        raise RuntimeError('Source operator does not support linkFrom()')


class BaseSinkBatchOp(BatchOperator, ABC):
    """
    Base class for sink batch operators.
    """

    def __init__(self, *args, **kwargs):
        """"""
        kwargs['OP_TYPE'] = 'SINK'
        super(BaseSinkBatchOp, self).__init__(*args, **kwargs)


class BatchOperatorWrapper(BatchOperator):
    """
    Wrap a Java object to a :py:class:`BatchOperator` instance.
    """

    def __init__(self, j_op: JavaObject):
        """
        Wrap a Java object of type `BatchOperator` to a :py:class:`BatchOperator` instance.

        :param j_op: the Java object.
        """
        super(BatchOperatorWrapper, self).__init__(j_op=j_op)
