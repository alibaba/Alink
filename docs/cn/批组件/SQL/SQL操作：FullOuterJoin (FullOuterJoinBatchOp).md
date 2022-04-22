# SQL操作：FullOuterJoin (FullOuterJoinBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.FullOuterJoinBatchOp

Python 类名：FullOuterJoinBatchOp


## 功能介绍
对批式数据进行sql的FULL OUTER JOIN操作。

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

df = pd.DataFrame([
    ['Ohio', 2000, 1.5],
    ['Ohio', 2001, 1.7],
    ['Ohio', 2002, 3.6],
    ['Nevada', 2001, 2.4],
    ['Nevada', 2002, 2.9],
    ['Nevada', 2003, 3.2]
])

batch_data1 = BatchOperator.fromDataframe(df, schemaStr='f1 string, f2 bigint, f3 double')
batch_data2 = BatchOperator.fromDataframe(df, schemaStr='f1 string, f2 bigint, f3 double')

op = FullOuterJoinBatchOp().setJoinPredicate("a.f1=b.f1 and a.f2=b.f2").setSelectClause("a.f1, a.f2, a.f3")
result = op.linkFrom(batch_data1, batch_data2)
result.print()
```

### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.FullOuterJoinBatchOp;
import org.junit.Test;

public class FullOuterJoinBatchOpTest {
	@Test
    public void testFullOuterJoinBatchOp() throws Exception {
    	List <Row> df = Arrays.asList(
    		Row.of("Ohio", 2000, 1.5),
    		Row.of("Ohio", 2001, 1.7),
    		Row.of("Ohio", 2002, 3.6),
    		Row.of("Nevada", 2001, 2.4),
    		Row.of("Nevada", 2002, 2.9),
    		Row.of("Nevada", 2003, 3.2)
    	);
    	BatchOperator <?> data1 = new MemSourceBatchOp(df, "f1 string, f2 int, f3 double");
    	BatchOperator <?> data2 = new MemSourceBatchOp(df, "f1 string, f2 int, f3 double");
    	BatchOperator <?> joinOp =
    		new FullOuterJoinBatchOp().setJoinPredicate("a.f1=b.f1 and a.f2=b.f2").setSelectClause(
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
Ohio|2000|1.5000
Ohio|2001|1.7000
Ohio|2002|3.6000
