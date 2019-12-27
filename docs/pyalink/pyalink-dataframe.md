与 Dataframe 互操作
==================

PyAlink 提供了与 [pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) 的互转操作，能够方便地使用 Python 生态中已有的强大工具。
pandas 的 DataFrame 是 Python 已有生态中表示、存储二维表的十分理想的选择，DataFrame 自身提供了一定的数据处理与可视化能力，同时又可以方便的转换为 Python 中的其他数据结构。


BatchOperator 转 Dataframe
------

对于各个 ```BatchOperator```，提供了 ```collectToDataframe()``` 的成员方法，将 ```BatchOperator``` 内的 ```DataSet``` 转换为 DataFrame；直接使用 ```print()``` 方法也可以直接以 ```DataFrame``` 的形式进行打印。

```
source = CsvSourceBatchOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")
res = source.select("sepal_length", "sepal_width")
df = res.collectToDataframe()
# Operations with df
res.print()
```

在 ```BatchOperator``` 中也提供了静态方法 ```collectoToDataframes``` 可以对多个 ```BatchOperator``` **同时**进行转换。 
每一次单个 ```BatchOperator``` 调用 ```collectoToDataframe``` 都会导致作业图进行一次计算，多次调用时将有效率问题，更重要的是，有些算法在多次调用时可能结果不一致，例如采样操作等。
此时可以使用这个静态方法来解决这些问题。

```
source = CsvSourceBatchOp() \
    .setSchemaStr(
    "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")

split = SplitBatchOp().setFraction(0.1).linkFrom(source)
[d0, d1, d2] = collectToDataframes(source, split, split.getSideOutput(0))
print(d0, d1, d2)
```

Dataframe 转 BatchOperator/StreamOperator
-------

对于用户拥有的 Dataframe 数据，PyAlink 也提供了向 BatchOperator/StreamOperator 转换的方法。
具体说来，```BatchOperator/StreamOperator ``` 中都具有静态方法 ```fromDataframe(df, schemaStr)```，其中 ```df``` 是 DataFrame 数据， ```schemaStr``` 是由数据列名和类型构成的 schema 字符串。

```
schema = "f_string string,f_long long,f_int int,f_double double,f_boolean boolean"
op = BatchOperator.fromDataframe(df, schema)
op.print()

op = StreamOperator.fromDataframe(df, schema)
op.print(key="op")
StreamOperator.execute()
```

同时，PyAlink 也提供了静态方法来进行转换：```dataframeToOperator(df, schemaStr, op_type)```，这里 ```df``` 和 ```schemaStr``` 参数与上文相同，```op_type``` 取值为 ```"batch"``` 或 ```"stream"```。


使用注意
-------
  - 从 BatchOperator 向 Dataframe 转换时：如果 BatchOperator 所存储的数据中有空值 ```null```，那么转换后的 Dataframe 将无法保证完全转换为 BatchOperator 的列类型。此时，对应的列需要手动进行处理。
  - 从 Dataframe 向 BatchOperator 转换时：如果 Dataframe 中有空值时，需要预先替换为 ```None```，这样才能正确地进行之后的处理。
  - 当数据量大时，由于数据转换需要进行大量的 Python 与 Java 数据之间的交换，效率将会受到影响。
