# 添加id列 (AppendIdBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp

Python 类名：AppendIdBatchOp


## 功能介绍

将表附加ID列

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| appendType | append类型 | append类型，"UNIQUE"和"DENSE"，分别为稀疏和稠密，稀疏的为非连续唯一id，稠密的为连续唯一id | String |  | "DENSE", "UNIQUE" | "DENSE" |
| idCol | ID列名 | ID列名 | String |  |  | "append_id" |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.0, "A", 0, 0, 0],
    [2.0, "B", 1, 1, 0],
    [3.0, "C", 2, 2, 1],
    [4.0, "D", 3, 3, 1]
])
inOp = BatchOperator.fromDataframe(df, schemaStr='f0 double,f1 string,f2 int,f3 int,label int')
AppendIdBatchOp()\
.setIdCol("append_id")\
.linkFrom(inOp)\
.print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AppendIdBatchOpTest {
	@Test
	public void testAppendIdBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.0, "A", 0, 0, 0),
			Row.of(2.0, "B", 1, 1, 0),
			Row.of(3.0, "C", 2, 2, 1),
			Row.of(4.0, "D", 3, 3, 1)
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "f0 double,f1 string,f2 int,f3 int,label int");
		new AppendIdBatchOp()
			.setIdCol("append_id")
			.linkFrom(inOp)
			.print();
	}
}
```

### 运行结果


|f0|f1|f2|f3|label|append_id|
|---|---|---|---|-----|---------|
|1.0000|A|0|0|0|0|
|2.0000|B|1|1|0|1|
|3.0000|C|2|2|1|2|
|4.0000|D|3|3|1|3|

