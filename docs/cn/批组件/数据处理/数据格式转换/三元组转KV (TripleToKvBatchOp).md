# 三元组转KV (TripleToKvBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.format.TripleToKvBatchOp

Python 类名：TripleToKvBatchOp


## 功能介绍
将数据格式从 Triple 转成 Key-value


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| tripleColumnCol | 三元组结构中列信息的列名 | 三元组结构中列信息的列名 | String | ✓ |  |
| tripleValueCol | 三元组结构中数据信息的列名 | 三元组结构中数据信息的列名 | String | ✓ |  |
| kvCol | KV列名 | KV列的列名 | String | ✓ |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR" |
| kvColDelimiter | 分隔符 | 当输入数据为稀疏格式时，key-value对之间的分隔符 | String |  | "," |
| kvValDelimiter | 分隔符 | 当输入数据为稀疏格式时，key和value的分割符 | String |  | ":" |
| tripleRowCol | 三元组结构中行信息的列名 | 三元组结构中行信息的列名 | String |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1,'f1',1.0],
    [1,'f2',2.0],
    [2,'f1',4.0],
    [2,'f2',8.0]])

data = BatchOperator.fromDataframe(df, schemaStr="row double, col string, val double")

op = TripleToKvBatchOp()\
    .setTripleRowCol("row")\
    .setTripleColumnCol("col")\
    .setTripleValueCol("val")\
    .setKvCol("kv")\
    .linkFrom(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.TripleToKvBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TripleToKvBatchOpTest {
	@Test
	public void testTripleToKvBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, "f1", 1.0),
			Row.of(1, "f2", 2.0),
			Row.of(2, "f1", 4.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "row int, col string, val double");
		BatchOperator <?> op = new TripleToKvBatchOp()
			.setTripleRowCol("row")
			.setTripleColumnCol("col")
			.setTripleValueCol("val")
			.setKvCol("kv")
			.linkFrom(data);
		op.print();
	}
}
```

### 运行结果
    
|row|kv|
|---|---|
|1|f1:1.0,f2:2.0|
|2|f1:4.0,f2:8.0|
    
