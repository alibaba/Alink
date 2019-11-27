StreamOperator 数据预览
=======

对于 StreamOperator， 在使用 Jupyter Notebook 时，PyAlink 提供了一种动态的数据预览方式。
这种预览方式采用了 DataFrame 的显示方式，支持随着时间窗口不断进行刷新，从而有较好的视觉体验来观察流式数据。

这种预览方式通过以下方法实现：
```print(self, key=None, refreshInterval=0, maxLimit=100)```

  - ```key``` 为一个字符串，表示给对应的 Operator 给定一个索引；不传值时将随机生成。
  - ```refreshInterval``` 表示刷新时间，单位为秒。当这个值大于0时，所显示的表将每隔 ```refreshInterval``` 秒刷新，显示前 ```refreshInterval``` 的数据；当这个值小于0时，每次有新数据产生，就会在触发显示，所显示的数据项与时间无关。
  - ```maxLimit``` 用于控制显示的数据量，最多显示 ```maxLimit``` 条数据。

```
schema = "age bigint, workclass string, fnlwgt bigint, education string, education_num bigint, marital_status string, occupation string, relationship string, race string, sex string, capital_gain bigint, capital_loss bigint, hours_per_week bigint, native_country string, label string"
adult_batch = CsvSourceStreamOp() \
    .setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/adult_train.csv") \
    .setSchemaStr(schema)
sample = SampleStreamOp().setRatio(0.01).linkFrom(adult_batch)
sample.print(key="adult_data", refreshInterval=3)
StreamOperator.execute()
```

需要特别注意的是：使用 ```print``` 进行数据预览的 StreamOperator 需要严格控制数据量。
单位时间数据量太大不仅不会对数据预览有太大帮助，还会造成计算与网络资源浪费。
同时， Python 端在收到数据后进行转换也是比较耗时的操作，两者会导致数据预览延迟。
比较合理的做法是通过采样组件 ```SampleStreamOp``` 来达到减少数据量的目的。
