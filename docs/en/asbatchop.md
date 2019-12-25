## Description
Rename the fields of a batch operator.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| clause | Operation clause. | String | ✓ |  |


## Script Example
#### Code

```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data = data.link(AsBatchOp().setClause("f1,f2,f3"))
```

