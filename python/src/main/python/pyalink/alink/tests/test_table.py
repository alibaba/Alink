import unittest

import pytest

from pyalink.alink import *


class TestEnvironment(unittest.TestCase):
    @pytest.mark.pyflink
    def test_batch_get_table(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        split = SplitBatchOp().setFraction(0.3).linkFrom(source)
        t0 = TableSourceBatchOp(split.getOutputTable())
        t1 = TableSourceBatchOp(split.getSideOutput(0).getOutputTable())
        df0, df1 = collectToDataframes(t0, t1)
        print(df0)
        print(df1)

    @pytest.mark.pyflink
    def test_stream_get_table(self):
        source = CsvSourceStreamOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        split = SplitStreamOp().setFraction(0.3).linkFrom(source)
        t0 = TableSourceStreamOp(split.getOutputTable())
        t1 = TableSourceStreamOp(split.getSideOutput(0).getOutputTable())
        t0.print()
        t1.print()
        StreamOperator.execute()
