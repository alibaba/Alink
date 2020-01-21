UDF 使用
=======

PyAlink 提供了基于 Python 的 UDF/UDTF 支持，方便进行灵活的数据处理。

UDF/UDTF 定义
------------
针对 Python 的语法，PyAlink 支持使用两种不同的形式来定义 UDF/UDTF：带有```eval```方法的类以及 lambda 方法。
对于 UDF，需要使用 ```return``` 来返回值；而 UDTF，则需要使用 ```yield``` 来多次返回值。
以下是使用这两种方式定义 UDF/UDTF 的代码示例：

```
# 类形式的 UDF
class PlusOne(object):
    def eval(self, x):
        return x + 1
    pass

# 类形式的 UDTF
class SplitOp(object):
    def eval(self, *args):
        for index, x in enumerate(args):
            yield index, x
    pass

# lambda 形式的 UDF
lambda x, y: x * 100 + y

# lambda 形式的 UDTF
lambda x, y: [ (yield x + 1 + i, y + 2 + i) for i in range(3) ]
```

UDF/UDTF 组件使用
---------

在流和批两种场景中，分别提供了 UDF/UDTF 对应的 Operator：
  - ```UDFBatchOp```
  - ```UDFStreamOp```
  - ```UDTFBatchOp```
  - ```UDTFStreamOp```

它们的参数包括：
  - ```setFunc```：设置 UDF 或 UDTF，例如 ```setFunc(PlusOne())``` 或者 ```setFunc(lambda x, y: x * 100 + y)```；
  - setSelectedCols：选择参与计算的列；
  - setOutputCol/setOutputCols：设置结果列名，其中 UDF 允许1列，UDTF 允许多列；
  - setResultType/setResultTypes：设置结果列类型；
  - setReservedCols：设置保留列；
  - setJoinType：UDTF 的 join 类型。

```
source = CsvSourceBatchOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")
udtfOp = UDTFBatchOp().setFunc(SplitOp()).setResultTypes(["LONG", "DOUBLE"]) \
    .setSelectedCols(['sepal_length', 'sepal_width']) \
    .setOutputCols(['index', 'x'])
udtf_res = udtfOp.linkFrom(source)
udtf_res.collectToDataframe()
```

除了使用组件形式以外，Operator 下还提供了```udf``` 和 ```udtf``` 方法，参数与上文中的 Operator 一致：
```
udf(self, func, selectedCols, outputCol, resultType, reservedCols=None)
udtf(self, func, selectedCols, outputCols, resultTypes, reservedCols=None)
```
