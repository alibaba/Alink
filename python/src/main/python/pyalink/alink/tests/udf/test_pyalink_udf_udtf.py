import unittest

from pyalink.alink import *
from pyalink.alink.udf.user_defined_function import UserDefinedFunction


class TestPyAlinkUdfUdtf(unittest.TestCase):

    def setUp(self) -> None:
        class PlusOne(UserDefinedFunction):
            def eval(self, x):
                return x + 10

            pass

        class SplitOp(UserDefinedFunction):
            def eval(self, *args):
                for index, x in enumerate(args):
                    yield index, x

            pass

        self.PlusOne = PlusOne
        self.SplitOp = SplitOp

    def test_udf(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        udf_op = UDFBatchOp() \
            .setFunc(self.PlusOne()) \
            .setResultType("DOUBLE") \
            .setSelectedCols(['sepal_length']) \
            .setOutputCol('sepal_length')
        res = udf_op.linkFrom(source)
        df = res.collectToDataframe()

        res.firstN(3).print()
        self.assertEqual(["sepal_width", "petal_length", "petal_width", "category", "sepal_length"], list(df.columns))

    def test_udtf(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        udtf_op = UDTFBatchOp() \
            .setFunc(self.SplitOp()) \
            .setResultTypes(["LONG", "DOUBLE"]) \
            .setSelectedCols(['sepal_length', 'sepal_width']) \
            .setOutputCols(['index', 'sepal_length'])
        udtf_res = udtf_op.linkFrom(source)
        df = udtf_res.collectToDataframe()

        udtf_res.firstN(4).print()
        self.assertEqual(['sepal_width', 'petal_length', 'petal_width', 'category', 'index', 'sepal_length'],
                          list(df.columns))

    def test_udf_lambda(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        udf_op2 = UDFBatchOp().setFunc(lambda x: x * 100).setResultType("DOUBLE").setSelectedCols(
            ['sepal_width']).setOutputCol('sepal_width').setReservedCols(['sepal_length'])
        res = udf_op2.linkFrom(source)
        df = res.collectToDataframe()
        res.firstN(3).print()
        self.assertEqual(["sepal_length", "sepal_width"], list(df.columns))

    def test_udtf_lambda(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        udtf_op2 = UDTFBatchOp().setFunc(
            lambda x, y: [(yield x + 1 + i, y + 2 + i) for i in range(3)]) \
            .setResultTypes(["DOUBLE", "DOUBLE"]) \
            .setSelectedCols(['sepal_length', 'sepal_width']) \
            .setOutputCols(['index', 'sepal_length'])
        udtf_res = udtf_op2.linkFrom(source.firstN(4))
        udtf_res.firstN(4).collectToDataframe()

        df = udtf_res.collectToDataframe()

        udtf_res.firstN(4).print()
        self.assertEqual(['sepal_width', 'petal_length', 'petal_width', 'category', 'index', 'sepal_length'],
                          list(df.columns))
