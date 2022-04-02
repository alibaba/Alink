import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestSqlQuery(unittest.TestCase):

    def test_batch(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        source.registerTableName("A")
        result = BatchOperator.sqlQuery("SELECT sepal_length FROM A")
        result.print()

    def test_batch2(self):
        data = np.array([
            ["1", 1, 1.1, 1.0, True],
            ["2", -2, 0.9, 2.0, False],
            ["3", 100, -0.01, 3.0, True],
            ["4", -99, None, 4.0, False],
            ["5", 1, 1.1, 5.0, True],
            ["6", -2, 0.9, 6.0, False]

        ])

        df = pd.DataFrame({"f1": data[:, 0], "f2": data[:, 1], "f3": data[:, 2], "f4": data[:, 3], "f5": data[:, 4]})
        data = dataframeToOperator(df, schemaStr='f1 string, f2 long, f3 double, f4 double, f5 boolean', opType='batch')
        data.print()

        data.registerTableName("t1")
        data.registerTableName("t2")
        res = BatchOperator.sqlQuery("select a.f1,b.f2 from t1 as a join t2 as b on a.f1=b.f1")
        res.print()

    def test_batch3(self):
        data = np.array([
            ["1", 1, 1.1, 1.0, True],
            ["2", -2, 0.9, 2.0, False],
            ["3", 100, -0.01, 3.0, True],
            ["4", -99, None, 4.0, False],
            ["5", 1, 1.1, 5.0, True],
            ["6", -2, 0.9, 6.0, False]
        ])

        df = pd.DataFrame({"f1": data[:, 0], "f2": data[:, 1], "f3": data[:, 2], "f4": data[:, 3], "f5": data[:, 4]})
        data = dataframeToOperator(df, schemaStr='f1 string, f2 long, f3 double, f4 double, f5 boolean', opType='batch')
        data.print()

        data.registerTableName("select")
        data.registerTableName("t2")
        res = BatchOperator.sqlQuery("select a.f1,b.f2 from `select` as a join t2 as b on a.f1=b.f1")
        res.print()

    def test_stream(self):
        source = CsvSourceStreamOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        source.registerTableName("A")
        result = source.sqlQuery("SELECT sepal_length FROM A")
        result.print()
        StreamOperator.execute()

    def test_stream2(self):
        data = np.array([
            ["1", 1, 1.1, 1.0, True],
            ["2", -2, 0.9, 2.0, False],
            ["3", 100, -0.01, 3.0, True],
            ["4", -99, None, 4.0, False],
            ["5", 1, 1.1, 5.0, True],
            ["6", -2, 0.9, 6.0, False]

        ])

        df = pd.DataFrame({"f1": data[:, 0], "f2": data[:, 1], "f3": data[:, 2], "f4": data[:, 3], "f5": data[:, 4]})
        data = dataframeToOperator(df, schemaStr='f1 string, f2 long, f3 double, f4 double, f5 boolean',
                                   opType='stream')
        data.print()

        data.registerTableName("t1")
        data.registerTableName("t2")
        res = StreamOperator.sqlQuery("select a.f1,b.f2 from t1 as a join t2 as b on a.f1=b.f1")
        res.print()
        StreamOperator.execute()
