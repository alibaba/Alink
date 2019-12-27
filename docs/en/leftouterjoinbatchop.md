## Description
Left outer join two batch operators.

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

joinOp = LeftOuterJoinBatchOp().setJoinPredicate("a.category=b.category").setSelectClause("a.petal_length")
output = joinOp.linkFrom(data1, data2)
```


<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| joinPredicate | where语句 | where语句 | String | ✓ |  |
| selectClause | select语句 | select语句 | String |  | "*" |
| type | join类型 | join类型: "join", "leftOuterJoin", "rightOuterJoin" 或 "fullOuterJoin" | String |  | "join" |<!-- This is the end of auto-generated parameter info -->

