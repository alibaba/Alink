import datetime
import unittest

import pandas as pd

from pyalink.alink import *
from pyalink.alink.common.types.mtable import MTable
from pyalink.alink.py4j_util import get_java_class


class TestMTable(unittest.TestCase):

    def test_mtable_methods_coverage(self):
        py_class = MTable
        j_cls_name = py_class._j_cls_name
        py_func_set = set(filter(
            lambda d: not d.startswith("_") and d != "get_j_obj" and d != "j_cls_name",
            dir(py_class)))
        py_func_set -= {'toDataframe', 'fromDataframe'}
        if hasattr(py_class, '_unsupported_j_methods'):
            py_func_set = py_func_set.union(set(py_class._unsupported_j_methods))

        j_check_wrapper_util_cls = get_java_class("com.alibaba.alink.python.utils.CheckWrapperUtil")
        j_func_set = set(j_check_wrapper_util_cls.getJMethodNames(j_cls_name))
        self.assertSetEqual(py_func_set, j_func_set)

    def test_mtable_init(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv") \
            .firstN(50)
        df = source.collectToDataframe()
        mtable: MTable = MTable.fromDataframe(df, source.getSchemaStr())
        self.assertEqual(type(mtable), MTable)
        self.assertIsNotNone(mtable.get_j_obj())
        self.assertEqual(len(mtable._df), 50)
        self.assertEqual(len(mtable._df.columns), 5)
        print(mtable)

    def test_mtable(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv") \
            .firstN(50)
        df = source.collectToDataframe()
        mtable: MTable = MTable.fromDataframe(df, source.getSchemaStr())
        self.assertEqual(type(mtable), MTable)

        # getColNames, getColTypes
        colnames = mtable.getColNames()
        self.assertListEqual(mtable.getColNames(), list(df.columns))
        self.assertListEqual(mtable.getColTypes(),
                             ['DOUBLE', 'DOUBLE', 'DOUBLE', 'DOUBLE', 'VARCHAR'])

        # getRow
        row_df = mtable.getRow(10)
        for i in range(len(colnames)):
            v = mtable.getEntry(10, i)
            self.assertEqual(v, row_df.iat[0, i])

        # getEntry
        for i in range(len(colnames)):
            v = mtable.getEntry(10, i)
            self.assertEqual(v, df.iat[10, i])

        # setEntry
        mtable2 = MTable.fromDataframe(mtable.toDataframe(), mtable.getSchemaStr())
        mtable2.setEntry(10, 4, "abc")
        self.assertEqual("abc", mtable2.getEntry(10, 4))

        # select
        mtable2: MTable = mtable.select((1, 2))
        self.assertEqual(type(mtable2), MTable)
        self.assertEqual(mtable2.getColNames(), [colnames[1], colnames[2]])
        self.assertEqual(mtable2.getNumRow(), mtable.getNumRow())

        mtable2: MTable = mtable.select((colnames[1], colnames[2]))
        self.assertEqual(type(mtable2), MTable)
        self.assertEqual(mtable2.getColNames(), [colnames[1], colnames[2]])
        self.assertEqual(mtable2.getNumRow(), mtable.getNumRow())

        # summary
        summary = mtable.summary((colnames[1], colnames[2]))
        self.assertTrue(isinstance(summary, TableSummary))
        self.assertEqual(summary.count(), 50)
        # TODO: check columns. Right now Java implementation is not right.

        summary = mtable.subSummary((colnames[1], colnames[2]), 10, 20)
        self.assertTrue(isinstance(summary, TableSummary))
        self.assertEqual(summary.count(), 10)
        # TODO: check columns. Right now Java implementation is not right.

        # orderBy
        mtable2 = MTable.fromDataframe(mtable.toDataframe(), mtable.getSchemaStr())
        mtable2.orderBy((colnames[1], colnames[2]), (True, False))
        # TODO: check results

        # toJson
        mtable2 = MTable.fromJson(mtable.toJson())
        self.assertEqual(type(mtable2), MTable)
        self.assertEqual(mtable2.toJson(), mtable.toJson())

        # copy
        mtable2 = mtable.copy()
        self.assertEqual(type(mtable2), MTable)
        self.assertEqual(mtable2.toJson(), mtable.toJson())

    def test_mtable_collect_print(self):
        source = BatchOperator.fromDataframe(pd.DataFrame([
            [1, datetime.datetime.fromtimestamp(1), 10.0],
            [1, datetime.datetime.fromtimestamp(2), 11.0],
            [1, datetime.datetime.fromtimestamp(3), 12.0],
            [1, datetime.datetime.fromtimestamp(4), 13.0],
            [1, datetime.datetime.fromtimestamp(5), 14.0],
            [1, datetime.datetime.fromtimestamp(6), 15.0],
            [1, datetime.datetime.fromtimestamp(7), 16.0],
            [1, datetime.datetime.fromtimestamp(8), 17.0],
            [1, datetime.datetime.fromtimestamp(9), 18.0],
            [1, datetime.datetime.fromtimestamp(10), 19.0]
        ]), schemaStr='id int, ts timestamp, val double')

        res: BatchOperator = GroupByBatchOp() \
            .setGroupByPredicate("id") \
            .setSelectClause("mtable_agg(ts, val) as data") \
            .linkFrom(source)
        res = res.select("data, 0 as v")  # multi-column
        # noinspection PyTypeChecker
        res = UnionAllBatchOp().linkFrom(res, res)  # multi-row
        df = res.collectToDataframe()
        print(df)  # in normal way
        self.assertEqual(type(df.iloc[0]['data']), MTable)
        res.print()  # in multi-lines way
