from pyalink.alink import *

benv, btenv, senv, stenv = useLocalEnv(2)

source = CsvSourceBatchOp() \
    .setSchemaStr(
    "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
    .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
source.registerTableName("A")

stream_source = CsvSourceStreamOp() \
    .setSchemaStr(
    "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
    .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
stream_source.registerTableName("A")


# ScalarFunction
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
f_udf4 = udf(lambda x, y: x + y + 40, input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()],
             result_type=DataTypes.DOUBLE())

udfs = [
    f_udf1,
    f_udf2,
    f_udf3,
    f_udf4
]

for index, f in enumerate(udfs):
    name = "plus" + str(index)
    print(name, f)
    BatchOperator.registerFunction(name, f)
    BatchOperator.sqlQuery("select " + name + "(sepal_width, petal_width) as t from A where sepal_width > 4").print()

for index, f in enumerate(udfs):
    name = "plus" + str(index)
    print(name, f)
    StreamOperator.registerFunction(name, f)
    StreamOperator.sqlQuery("select " + name + "(sepal_width, petal_width) as t from A where sepal_width > 4").print()
    StreamOperator.execute()

for index, f in enumerate(udfs):
    udfBatchOp = UDFBatchOp() \
        .setFunc(f) \
        .setSelectedCols(["sepal_length", "sepal_width"]) \
        .setOutputCol("sepal_length") \
        .linkFrom(source)
    df = udfBatchOp.collectToDataframe()
    print(df)

for index, f in enumerate(udfs):
    udfStreamOp = UDFStreamOp() \
        .setFunc(f) \
        .setSelectedCols(["sepal_length", "sepal_width"]) \
        .setOutputCol("sepal_length") \
        .linkFrom(stream_source)
    udfStreamOp.print()
    StreamOperator.execute()


# TableFunction
class SplitOp(TableFunction):
    def eval(self, *args):
        for i, arg in enumerate(args):
            yield i, arg


f_udtf1 = udtf(SplitOp(), [DataTypes.DOUBLE(), DataTypes.DOUBLE()], [DataTypes.INT(), DataTypes.DOUBLE()])


# function + decorator
@udtf(input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_types=[DataTypes.INT(), DataTypes.DOUBLE()])
def f_udtf2(*args):
    for i, arg in enumerate(args):
        yield i, arg


# function
def f_udtf3(*args):
    for i, arg in enumerate(args):
        yield i, arg


f_udtf3 = udtf(f_udtf3, input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()],
               result_types=[DataTypes.INT(), DataTypes.DOUBLE()])

# lambda function
f_udtf4 = udtf(lambda *args: [(yield i, arg) for i, arg in enumerate(args)],
               input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_types=[DataTypes.INT(), DataTypes.DOUBLE()])

udtfs = [
    f_udtf1,
    f_udtf2,
    f_udtf3,
    f_udtf4
]

for index, f in enumerate(udtfs):
    name = "split" + str(index)
    print(name, f)
    BatchOperator.registerFunction(name, f)
    BatchOperator.sqlQuery(
        "select sepal_width, index, v from A, LATERAL TABLE(" + name + "(sepal_width, petal_length)) as T(index, v) where sepal_width > 4").print()

for index, f in enumerate(udtfs):
    name = "split" + str(index)
    print(name, f)
    StreamOperator.registerFunction(name, f)
    StreamOperator.sqlQuery(
        "select sepal_width, index, v from A, LATERAL TABLE(" + name + "(sepal_width, petal_length)) as T(index, v) where sepal_width > 4").print()
    StreamOperator.execute()

for index, f in enumerate(udtfs):
    udtfBatchOp = UDTFBatchOp() \
        .setFunc(f) \
        .setSelectedCols(["sepal_length", "sepal_width"]) \
        .setOutputCols(["index", "sepal_length"]) \
        .linkFrom(source)
    df = udtfBatchOp.collectToDataframe()
    print(df)

for index, f in enumerate(udtfs):
    udtfStreamOp = UDTFStreamOp() \
        .setFunc(f) \
        .setSelectedCols(["sepal_length", "sepal_width"]) \
        .setOutputCols(["index", "sepal_length"]) \
        .linkFrom(stream_source)
    udtfStreamOp.print()
    StreamOperator.execute()
