import datetime
import time
import unittest

import numpy as np
import scipy.sparse.linalg

from pyalink.alink import *


class TestUdf(unittest.TestCase):

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
        class PlusOne(ScalarFunction):
            def eval(self, x, y):
                return x + y + 10

        f_udf1 = udf(PlusOne(), input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.DOUBLE())

        # function + decorator
        @udf(input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.DOUBLE())
        def f_udf2(x, y):
            return x + y + 20

        # function
        def f_udf3(x, y):
            return x + y + 30

        f_udf3 = udf(f_udf3, input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.DOUBLE())

        # lambda function
        f_udf4 = udf(lambda x, y: x + y + 40
                     , input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.DOUBLE())

        self.udfs = [
            f_udf1,
            f_udf2,
            f_udf3,
            f_udf4
        ]

    def test_udf_batch_op(self):
        for index, f in enumerate(self.udfs):
            udfBatchOp = UDFBatchOp() \
                .setFunc(f) \
                .setSelectedCols(["sepal_length", "sepal_width"]) \
                .setOutputCol("sepal_length") \
                .linkFrom(self.source)
            df = udfBatchOp.collectToDataframe()
            print(df)
            self.assertEqual(['sepal_width', 'petal_length', 'petal_width', 'category', 'sepal_length'],
                              list(df.columns))

    def test_udf_stream_op(self):
        for index, f in enumerate(self.udfs):
            udfStreamOp = UDFStreamOp() \
                .setFunc(f) \
                .setSelectedCols(["sepal_length", "sepal_width"]) \
                .setOutputCol("sepal_length") \
                .linkFrom(self.stream_source)
            udfStreamOp.print()
            StreamOperator.execute()
            self.assertEqual(['sepal_width', 'petal_length', 'petal_width', 'category', 'sepal_length'],
                              udfStreamOp.getColNames())

    def test_batch_sql_query(self):
        for index, f in enumerate(self.udfs):
            name = "plus" + str(index)
            print(name, f)
            BatchOperator.registerFunction(name, f)
            res = BatchOperator.sqlQuery(
                "select " + name + "(sepal_width, petal_width) as t, petal_width from A where sepal_width > 4")
            df = res.collectToDataframe()
            print(df)
            self.assertEqual(['t', 'petal_width'], list(df.columns))

    def test_stream_sql_query(self):
        for index, f in enumerate(self.udfs):
            name = "plus" + str(index)
            print(name, f)
            StreamOperator.registerFunction(name, f)
            res = StreamOperator.sqlQuery(
                "select " + name + "(sepal_width, petal_width) as t, petal_width from A where sepal_width > 4")
            res.print()
            StreamOperator.execute()
            self.assertEqual(['t', 'petal_width'], res.getColNames())

    def test_batch_op_udf(self):
        for index, f in enumerate(self.udfs):
            res = self.source.udf(f, ["sepal_length", "sepal_width"], "sepal_length")
            df = res.collectToDataframe()
            print(df)
            self.assertEqual(['sepal_width', 'petal_length', 'petal_width', 'category', 'sepal_length'],
                              list(df.columns))

    def test_stream_op_udf(self):
        for index, f in enumerate(self.udfs):
            res = self.stream_source.udf(f, ["sepal_length", "sepal_width"], "sepal_length")
            res.print()
            StreamOperator.execute()
            self.assertEqual(['sepal_width', 'petal_length', 'petal_width', 'category', 'sepal_length'],
                              res.getColNames())

    def test_udf_timestamp(self):
        @udf(input_types=[DataTypes.DOUBLE()], result_type=DataTypes.TIMESTAMP(3))
        def to_timestamp(x):
            return datetime.datetime.fromtimestamp(time.time())
        df = self.source.udf(to_timestamp, ["sepal_length"], "timestamp").collectToDataframe()
        self.assertEqual(df.dtypes['timestamp'], np.dtype('datetime64[ns]'))
        print(df)
        self.stream_source.udf(to_timestamp, ["sepal_length"], "timestamp").print()
        StreamOperator.execute()

    def test_udf_date(self):
        @udf(input_types=[DataTypes.DOUBLE()], result_type=DataTypes.DATE())
        def to_date(x):
            return datetime.datetime.fromtimestamp(time.time()).date()
        self.source.udf(to_date, ["sepal_length"], "date").print()
        self.stream_source.udf(to_date, ["sepal_length"], "date").print()

    def test_udf_time(self):
        @udf(input_types=[DataTypes.DOUBLE()], result_type=DataTypes.TIME())
        def to_date(x):
            return datetime.datetime.fromtimestamp(time.time()).time()
        self.source.udf(to_date, ["sepal_length"], "time").print()
        self.stream_source.udf(to_date, ["sepal_length"], "time").print()

    def test_tensor_udf(self):
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
                return np.ones([3, 5, 6], dtype="<i4")
                # return x + y + 10

        f_udf1 = udf(PlusOne(), input_types=[AlinkDataTypes.TENSOR(), AlinkDataTypes.TENSOR()],
                     result_type=AlinkDataTypes.TENSOR())

        for index, f in enumerate([f_udf1]):
            udfBatchOp = UDFBatchOp() \
                .setFunc(f) \
                .setSelectedCols(["vec", "vec"]) \
                .setOutputCol("sum") \
                .linkFrom(tensor)
            print(udfBatchOp.getSchemaStr())
            df = udfBatchOp.collectToDataframe()
            print(df)

    def test_vector_udf(self):
        AlinkGlobalConfiguration.setPrintProcessInfo(True)
        import pandas as pd
        df = pd.DataFrame([['1', '{"1":"1.0","2":"2.0"}']])
        source = BatchOperator.fromDataframe(df, schemaStr="row string, json string")
        vector = JsonToVectorBatchOp() \
            .setJsonCol("json") \
            .setReservedCols(["row"]) \
            .setVectorCol("vec") \
            .setVectorSize(5) \
            .linkFrom(source)
        vector.print()

        class VecNorm(ScalarFunction):
            def eval(self, x):
                # Note: Java SparseVector is converted to Scipy sparse matrix which has 2 dimensions
                print(type(x), flush=True)  # a subclass of scipy.sparse.spmatrix
                return scipy.sparse.linalg.norm(x)

        f_udf1 = udf(VecNorm(), input_types=[AlinkDataTypes.VECTOR()], result_type=AlinkDataTypes.DOUBLE())

        for index, f in enumerate([f_udf1]):
            udfBatchOp = UDFBatchOp() \
                .setFunc(f) \
                .setSelectedCols(["vec"]) \
                .setOutputCol("norm") \
                .linkFrom(vector)
            print(udfBatchOp.getSchemaStr())
            df = udfBatchOp.collectToDataframe()
            print(df)

    def test_mtable_udf(self):
        import pandas as pd
        df = pd.DataFrame([['{"data":{"col0":[1],"col1":["2"],"label":[0],"ts":["2603-10-12 04:13:52.012"],"d_vec":['
                            'null],"s_vec":["$3$1:2.0"],"tensor":["FLOAT#1#3.0 "]},"schema":"col0 INT,col1 VARCHAR,'
                            'label INT,ts TIMESTAMP,d_vec DENSE_VECTOR,s_vec VECTOR,tensor FLOAT_TENSOR"}']])
        source = BatchOperator.fromDataframe(df, schemaStr="content string")
        mtable = ToMTableBatchOp() \
            .setSelectedCol("content")\
            .setOutputCol("mtable") \
            .linkFrom(source)
        mtable.print()

        class MTableOp(ScalarFunction):
            def eval(self, df: pd.DataFrame):
                df['col0'] += 10
                return df

        f_udf1 = udf(MTableOp(), input_types=[AlinkDataTypes.MTABLE()], result_type=AlinkDataTypes.MTABLE())

        for index, f in enumerate([f_udf1]):
            udfBatchOp = UDFBatchOp() \
                .setFunc(f) \
                .setSelectedCols(["mtable"]) \
                .setOutputCol("out") \
                .linkFrom(mtable)
            print(udfBatchOp.getSchemaStr())
            df = udfBatchOp.collectToDataframe()
            print(df)
