# SQL操作：FullOuterJoin (FullOuterJoinBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.FullOuterJoinBatchOp

Python 类名：FullOuterJoinBatchOp


## 功能介绍
提供sql的full outer join语句功能

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| joinPredicate | where语句 | where语句 | String | ✓ |  |
| selectClause | select语句 | select语句 | String | ✓ |  |
| type | join类型 | join类型: "join", "leftOuterJoin", "rightOuterJoin" 或 "fullOuterJoin" | String |  | "JOIN" |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data1 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data2 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

joinOp = FullOuterJoinBatchOp().setJoinPredicate("a.category=b.category").setSelectClause("a.petal_length")
output = joinOp.linkFrom(data1, data2)
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.FullOuterJoinBatchOp;
import org.junit.Test;

public class FullOuterJoinBatchOpTest {
	@Test
	public void testFullOuterJoinBatchOp() throws Exception {
		String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		BatchOperator <?> data1 = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		BatchOperator <?> data2 = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		BatchOperator <?> joinOp =
			new FullOuterJoinBatchOp().setJoinPredicate("a.category=b.category").setSelectClause(
			"a.petal_length");
		joinOp.linkFrom(data1, data2).print();
	}
}
```


<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| joinPredicate | where语句 | where语句 | String | ✓ |  |
| selectClause | select语句 | select语句 | String |  | "*" |
| type | join类型 | join类型: "join", "leftOuterJoin", "rightOuterJoin" 或 "fullOuterJoin" | String |  | "join" |<!-- This is the end of auto-generated parameter info -->

### 运行结果

petal_length|
------------|
4.5000|
4.5000|
4.5000|
4.5000|
4.5000|
... |
1.9000|
1.9000|
1.9000|
1.9000|
1.9000|
