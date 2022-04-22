# SQL操作：As (AsBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.AsBatchOp

Python 类名：AsBatchOp


## 功能介绍
对批式数据进行sql的AS操作。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |  |



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

batch_data = BatchOperator.fromDataframe(df, schemaStr='f1 string, f2 bigint, f3 double')
op = AsBatchOp().setClause("ff1,ff2,ff3")
batch_data = batch_data.link(op)
batch_data.print()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.AsBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AsBatchOpTest {
	@Test
	public void testAsBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		BatchOperator <?> batch_data = new MemSourceBatchOp(df, "f1 string, f2 int, f3 double");
		BatchOperator <?> op = new AsBatchOp().setClause("ff1,ff2,ff3");
		batch_data = batch_data.link(op);
		batch_data.print();
	}
}
```

### 运行结果
ff1|ff2|ff3
---|---|---
Ohio|2000|1.5000
Ohio|2001|1.7000
Ohio|2002|3.6000
Nevada|2001|2.4000
Nevada|2002|2.9000
Nevada|2003|3.2000
