## Description
Left outer join two batch operators.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| joinPredicate | joinPredicate | String | ✓ |  |
| selectClause | Select clause | String | ✓ |  |
| type | Join type, one of "join", "leftOuterJoin", "rightOuterJoin", "fullOuterJoin" | String |  | "JOIN" |

## Script Example
#### Code

```python
URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data1 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data2 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

joinOp = LeftOuterJoinBatchOp().setJoinPredicate("a.category=b.category").setSelectClause("a.petal_length")
output = joinOp.linkFrom(data1, data2)
```
