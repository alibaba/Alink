## Description
Minus another <code>BatchOperator</code>. The duplicated records are kept.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |


## Script Example
#### Code

```python
URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data1 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data2 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

minusAllOp = MinusAllBatchOp()
output = minusAllOp.linkFrom(data1, data2)
```
