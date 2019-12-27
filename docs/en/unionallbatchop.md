## Description
Union with another <code>BatchOperator</code>. The duplicated records are kept.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |



## Script Example
#### Code

```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data1 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data2 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

unionAllOp = UnionAllBatchOp()
output = unionAllOp.linkFrom(data1, data2)
```

