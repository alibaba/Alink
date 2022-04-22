# SQL操作：Join (JoinBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.JoinBatchOp

Python 类名：JoinBatchOp


## 功能介绍
对批式数据进行sql的JOIN操作。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| joinPredicate | where语句 | where语句 | String | ✓ |  |  |
| selectClause | select语句 | select语句 | String | ✓ |  |  |
| type | join类型 | join类型: "join", "leftOuterJoin", "rightOuterJoin" 或 "fullOuterJoin" | String |  | "JOIN", "LEFTOUTERJOIN", "RIGHTOUTERJOIN", "FULLOUTERJOIN" | "JOIN" |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df1 = pd.DataFrame([
    ['Ohio', 2000, 1.5],
    ['Ohio', 2001, 1.7],
    ['Ohio', 2002, 3.6],
    ['Nevada', 2001, 2.4],
    ['Nevada', 2002, 2.9],
    ['Nevada', 2003, 3.2]
])
df2 = pd.DataFrame([
    ['Nevada', 2001, 2.4],
    ['Nevada', 2003, 3.2]
])

batch_data1 = BatchOperator.fromDataframe(df1, schemaStr='f1 string, f2 bigint, f3 double')
batch_data2 = BatchOperator.fromDataframe(df2, schemaStr='f1 string, f2 bigint, f3 double')

op = JoinBatchOp().setJoinPredicate("a.f1=b.f1").setSelectClause("a.f1, a.f2, a.f3")
op.linkFrom(batch_data1, batch_data2).print()
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.JoinBatchOp;
import org.junit.Test;

public class JoinBatchOpTest {
	@Test
    public void testJoinBatchOp() throws Exception {
    	List <Row> df1 = Arrays.asList(
        	Row.of("Ohio", 2000, 1.5),
    		Row.of("Ohio", 2001, 1.7),
    		Row.of("Ohio", 2002, 3.6),
    		Row.of("Nevada", 2001, 2.4),
    		Row.of("Nevada", 2002, 2.9),
    		Row.of("Nevada", 2003, 3.2)
    	);
    	List <Row> df2 = Arrays.asList(
    		Row.of("Nevada", 2001, 2.4),
    		Row.of("Nevada", 2003, 3.2)
    	);
    	BatchOperator <?> data1 = new MemSourceBatchOp(df1, "f1 string, f2 int, f3 double");
    	BatchOperator <?> data2 = new MemSourceBatchOp(df2, "f1 string, f2 int, f3 double");
    	BatchOperator <?> joinOp = new JoinBatchOp().setJoinPredicate("a.f1=b.f1").setSelectClause(
    		"a.f1, a.f2, a.f3");
    	joinOp.linkFrom(data1, data2).print();
	}
}
```

### 运行结果

f1|f2|f3
---|---|---
Nevada|2001|2.4000
Nevada|2002|2.9000
Nevada|2003|3.2000
Nevada|2001|2.4000
Nevada|2002|2.9000
Nevada|2003|3.2000
