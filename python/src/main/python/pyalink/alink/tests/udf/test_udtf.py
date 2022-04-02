import datetime
import time
import unittest

from pyalink.alink import *


class TestUdtf(unittest.TestCase):

    def setUp(self) -> None:
        self.source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        self.source.registerTableName("A")

        self.stream_source = CsvSourceStreamOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        self.stream_source.registerTableName("A")

        # TableFunction
        class SplitOp(TableFunction):
            def eval(self, *args):
                for index, arg in enumerate(args):
                    yield index, arg

        f_udtf1 = udtf(SplitOp(), [DataTypes.DOUBLE(), DataTypes.DOUBLE()], [DataTypes.INT(), DataTypes.DOUBLE()])

        # function + decorator
        @udtf(input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_types=[DataTypes.INT(), DataTypes.DOUBLE()])
        def f_udtf2(*args):
            for index, arg in enumerate(args):
                yield index, arg

        # function
        def f_udtf3(*args):
            for index, arg in enumerate(args):
                yield index, arg

        f_udtf3 = udtf(f_udtf3, input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()],
                       result_types=[DataTypes.INT(), DataTypes.DOUBLE()])

        # lambda function
        f_udtf4 = udtf(lambda *args: [(yield index, arg) for index, arg in enumerate(args)]
                       , input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()],
                       result_types=[DataTypes.INT(), DataTypes.DOUBLE()])

        self.udtfs = [
            f_udtf1,
            f_udtf2,
            f_udtf3,
            f_udtf4
        ]

    def test_udtf_batch_op(self):
        for index, f in enumerate(self.udtfs):
            udtfBatchOp = UDTFBatchOp() \
                .setFunc(f) \
                .setSelectedCols(["sepal_length", "sepal_width"]) \
                .setOutputCols(["index", "sepal_length"]) \
                .linkFrom(self.source)
            df = udtfBatchOp.collectToDataframe()
            print(df)
            self.assertEqual(['sepal_width', 'petal_length', 'petal_width', 'category', 'index', 'sepal_length'],
                              list(df.columns))

    def test_udtf_stream_op(self):
        for index, f in enumerate(self.udtfs):
            udtfStreamOp = UDTFStreamOp() \
                .setFunc(f) \
                .setSelectedCols(["sepal_length", "sepal_width"]) \
                .setOutputCols(["index", "sepal_length"]) \
                .linkFrom(self.stream_source)
            udtfStreamOp.print()
            StreamOperator.execute()
            self.assertEqual(['sepal_width', 'petal_length', 'petal_width', 'category', 'index', 'sepal_length'],
                              udtfStreamOp.getColNames())

    def test_batch_sql_query(self):
        for index, f in enumerate(self.udtfs):
            name = "split" + str(index)
            print(name, f)
            BatchOperator.registerFunction(name, f)
            res = BatchOperator.sqlQuery(
                "select sepal_width, index, v from A, LATERAL TABLE(" + name + "(sepal_width, petal_length)) as T(index, v) where sepal_width > 4")
            df = res.collectToDataframe()
            print(df)
            self.assertEqual(['sepal_width', 'index', 'v'], list(df.columns))

    def test_stream_sql_query(self):
        for index, f in enumerate(self.udtfs):
            name = "split" + str(index)
            print(name, f)
            StreamOperator.registerFunction(name, f)
            res = StreamOperator.sqlQuery(
                "select sepal_width, index, v from A, LATERAL TABLE(" + name + "(sepal_width, petal_length)) as T(index, v) where sepal_width > 4")
            res.print()
            StreamOperator.execute()
            self.assertEqual(['sepal_width', 'index', 'v'], res.getColNames())

    def test_batch_op_udtf(self):
        for index, f in enumerate(self.udtfs):
            res = self.source.udtf(f, ["sepal_length", "sepal_width"], ["index", "sepal_length"])
            df = res.collectToDataframe()
            print(df)
            self.assertEqual(['sepal_width', 'petal_length', 'petal_width', 'category', 'index', 'sepal_length'],
                              list(df.columns))

    def test_stream_op_udtf(self):
        for index, f in enumerate(self.udtfs):
            res = self.stream_source.udtf(f, ["sepal_length", "sepal_width"], ["index", "sepal_length"])
            res.print()
            StreamOperator.execute()
            self.assertEqual(['sepal_width', 'petal_length', 'petal_width', 'category', 'index', 'sepal_length'],
                              res.getColNames())

    def test_udtf_timestamp(self):
        @udtf(input_types=[DataTypes.DOUBLE()], result_types=[DataTypes.TIMESTAMP(3)])
        def to_timestamp(x):
            yield datetime.datetime.fromtimestamp(time.time())
        self.source.udtf(to_timestamp, ["sepal_length"], ["timestamp"]).print()
        self.stream_source.udtf(to_timestamp, ["sepal_length"], ["timestamp"]).print()
        StreamOperator.execute()

    def test_udtf_date(self):
        @udtf(input_types=[DataTypes.DOUBLE()], result_types=[DataTypes.DATE()])
        def to_date(x):
            yield datetime.datetime.fromtimestamp(time.time()).date()
        self.source.udtf(to_date, ["sepal_length"], ["date"]).print()
        self.stream_source.udtf(to_date, ["sepal_length"], ["date"]).print()
        StreamOperator.execute()

    def test_udtf_time(self):
        @udtf(input_types=[DataTypes.DOUBLE()], result_types=[DataTypes.TIME()])
        def to_date(x):
            yield datetime.datetime.fromtimestamp(time.time()).time()
        self.source.udtf(to_date, ["sepal_length"], ["time"]).print()
        self.stream_source.udtf(to_date, ["sepal_length"], ["time"]).print()
        StreamOperator.execute()

    def test_udf_tensor(self):
        import pandas as pd
        df = pd.DataFrame(["FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "])
        source = BatchOperator.fromDataframe(df, schemaStr='vec string')
        tensor = ToTensorBatchOp() \
            .setSelectedCol("vec") \
            .setTensorShape([2, 3]) \
            .setTensorDataType("float") \
            .linkFrom(source)
        tensor.print()

        class PlusOne(ScalarFunction):
            def eval(self, x, y):
                for i in range(5):
                    yield i, x + y + i

        f_udf1 = udtf(PlusOne(), input_types=[AlinkDataTypes.TENSOR(), AlinkDataTypes.TENSOR()],
                      result_types=[DataTypes.INT(), AlinkDataTypes.TENSOR()])

        for index, f in enumerate([f_udf1]):
            udtfBatchOp = UDTFBatchOp() \
                .setFunc(f) \
                .setSelectedCols(["vec", "vec"]) \
                .setOutputCols(["index", "sum"]) \
                .linkFrom(tensor)
            df = udtfBatchOp.collectToDataframe()
            print(df)
