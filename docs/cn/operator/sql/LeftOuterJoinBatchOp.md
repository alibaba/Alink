## 功能介绍
提供sql的left outer join语句功能

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| joinPredicate | where语句 | where语句 | String | ✓ |  |
| selectClause | select语句 | select语句 | String | ✓ |  |
| type | join类型 | join类型: "join", "leftOuterJoin", "rightOuterJoin" 或 "fullOuterJoin" | String |  | "JOIN" |

## 脚本示例
#### 脚本代码

```python
URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data1 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data2 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

joinOp = LeftOuterJoinBatchOp().setJoinPredicate("a.category=b.category").setSelectClause("a.petal_length")
output = joinOp.linkFrom(data1, data2)
```
