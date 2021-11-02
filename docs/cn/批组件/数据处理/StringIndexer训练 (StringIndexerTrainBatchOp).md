# StringIndexer训练 (StringIndexerTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp

Python 类名：StringIndexerTrainBatchOp


## 功能介绍
StringIndexer训练组件的作用是训练一个模型用于将单列字符串映射为整数。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| stringOrderType | Token排序方法 | Token排序方法 | String |  | "RANDOM" |
| modelName | 模型名字 | 模型名字 | String |  |  |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["football"],
    ["football"],
    ["football"],
    ["basketball"],
    ["basketball"],
    ["tennis"],
])


data = BatchOperator.fromDataframe(df, schemaStr='f0 string')

stringindexer = StringIndexerTrainBatchOp() \
    .setSelectedCol("f0") \
    .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)
model.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StringIndexerTrainBatchOpTest {
	@Test
	public void testStringIndexerTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football"),
			Row.of("football"),
			Row.of("football"),
			Row.of("basketball"),
			Row.of("basketball"),
			Row.of("tennis")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string");
		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setStringOrderType("frequency_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		model.print();
	}
}
```

### 运行结果

模型表：

token|token_index
-----|-----------
tennis|0
basketball|1
football|2


