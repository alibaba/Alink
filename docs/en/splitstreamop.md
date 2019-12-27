## Description
Split a stream data into two parts.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| fraction | Proportion of data allocated to left output after splitting | Double | ✓ |  |


## Script Example
```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
spliter = SplitStreamOp().setFraction(0.4)
train_data = spliter
test_data = spliter.getSideOutput(0)
```

