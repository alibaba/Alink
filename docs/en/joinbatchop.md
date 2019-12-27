## Description
Join two batch operators.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| joinPredicate | joinPredicate | String | ✓ |  |
| selectClause | Select clause | String |  | "*" |
| type | Join type, one of "join", "leftOuterJoin", "rightOuterJoin", "fullOuterJoin" | String |  | "join" |


## Script Example
#### Code

```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data1 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data2 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

joinOp = JoinBatchOp().setJoinPredicate("a.category=b.category").setSelectClause("a.petal_length")
output = joinOp.linkFrom(data1, data2)
```

