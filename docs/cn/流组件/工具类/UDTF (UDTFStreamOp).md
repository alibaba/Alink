# UDTF (UDTFStreamOp)
Java 类名：com.alibaba.alink.operator.stream.utils.UDTFStreamOp

Python 类名：UDTFStreamOp


## UDF/UDTF 定义

PyAlink 提供了基于 Python 的 UDF/UDTF 支持，方便进行灵活的数据处理。
PyAlink 所定义的 UDF/UDTF 即可以用于 PyAlink 提供的 UDF/UDTF 组件，也可以用于所提供的 `sqlQuery` 函数。

我们提供了 `udf` 和 `udtf` 函数来帮助构造 UDF/UDTF。
两个函数使用时都需要提供一个函数体、输入类型和返回类型。
- 函数体对于 UDF 而言，是直接用 `return` 返回值的 Python 函数，或者 lambda 函数；
  对于 UDTF 而言，是用 `yield` 来多次返回值的 Python 函数。
- 输入类型均为 `DataType` 类型的 Python list。
- 输出类型，UDF 为单个 `DataType` 类型，UDTF为 `DataType` 类型的 Python list。

`DataType` 类型可以直接用`DataTypes.DOUBLE()`等类似的函数得到。

以下是定义 UDF/UDTF 的代码示例：

```
# 4种 UDF 定义

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
f_udf4 = udf(lambda x, y: x + y + 40
               , input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.DOUBLE())

udfs = [
    f_udf1,
    f_udf2,
    f_udf3,
    f_udf4
]

# 4种 UDTF 定义

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
f_udtf3 = udtf(f_udtf3, input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_types=[DataTypes.INT(), DataTypes.DOUBLE()])

# lambda function
f_udtf4 = udtf(lambda *args: [ (yield index, arg) for index, arg in enumerate(args) ]
               , input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_types=[DataTypes.INT(), DataTypes.DOUBLE()])

udtfs = [
    f_udtf1,
    f_udtf2,
    f_udtf3,
    f_udtf4
]
```

## UDF/UDTF 组件使用

在流和批两种场景中，分别提供了 UDF/UDTF 对应的 Operator：
- ```UDFBatchOp```
- ```UDFStreamOp```
- ```UDTFBatchOp```
- ```UDTFStreamOp```

它们的参数包括：
- ```setFunc```：设置 UDF 或 UDTF，由前文的 `udf` 或 `udtf` 函数产生；
- setSelectedCols：选择参与计算的列；
- setOutputCol/setOutputCols：设置结果列名，其中 UDF 允许1列，UDTF 允许多列；
- setReservedCols：设置保留列。

```
source = CsvSourceBatchOp() \
    .setSchemaStr(
    "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
    .setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv")

for index, f in enumerate(udfs):
    udfBatchOp = UDFBatchOp() \
        .setFunc(f) \
        .setSelectedCols(["sepal_length", "sepal_width"]) \
        .setOutputCol("sepal_length") \
        .linkFrom(source)
    df = udfBatchOp.collectToDataframe()
    print(df)

stream_source = CsvSourceStreamOp() \
    .setSchemaStr(
    "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
    .setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv")

for index, f in enumerate(udtfs):
    udtfStreamOp = UDTFStreamOp() \
        .setFunc(f) \
        .setSelectedCols(["sepal_length", "sepal_width"]) \
        .setOutputCols(["index", "sepal_length"]) \
        .linkFrom(stream_source)
    udtfStreamOp.print()
    StreamOperator.execute()
```

除了使用组件形式以外，Operator 下还提供了```udf``` 和 ```udtf``` 方法，参数与上文中的 Operator 一致：
```
udf(self, func, selectedCols, outputCol, resultType, reservedCols=None)
udtf(self, func, selectedCols, outputCols, resultTypes, reservedCols=None)
```



## SQL 中使用 UDF/UDTF

PyAlink 提供了更多对于 SQL 的支持。
`BatchOperator` 和 `StreamOperator`提供了 `registerTableName` 和 `registerFunction` 的方法，用于将 Operator 对应的 Table 和 UDF/UDTF 注册。
`BatchOperator` 和 `StreamOperator` 还提供了 `sqlQuery` 静态函数来支持 SQL 功能。

SQL的使用可以参考下面的代码：

```
source.registerTableName("A")
for index, f in enumerate(udfs):
    name = "plus" + str(index)
    print(name, f)
    BatchOperator.registerFunction(name, f)
    BatchOperator.sqlQuery("select " + name + "(sepal_width, petal_width) as t from A where sepal_width > 4").print()

stream_source.registerTableName("A")
for index, f in enumerate(udtfs):
    name = "split" + str(index)
    print(name, f)
    StreamOperator.registerFunction(name, f)
    StreamOperator.sqlQuery("select sepal_width, index, v from A, LATERAL TABLE(" + name + "(sepal_width, petal_length)) as T(index, v) where sepal_width > 4").print()
    StreamOperator.execute()
```