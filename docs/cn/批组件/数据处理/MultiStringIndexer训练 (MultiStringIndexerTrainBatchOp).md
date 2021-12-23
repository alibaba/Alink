# MultiStringIndexer训练 (MultiStringIndexerTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp

Python 类名：MultiStringIndexerTrainBatchOp


## 功能介绍
MultiStringIndexer训练组件的作用是训练一个模型用于将多列字符串映射为整数。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| stringOrderType | Token排序方法 | Token排序方法 | String |  | "RANDOM" |



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

stringindexer = MultiStringIndexerTrainBatchOp() \
    .setSelectedCols(["f0"]) \
    .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)
model.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MultiStringIndexerTrainBatchOpTest {
	@Test
	public void testMultiStringIndexerTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football"),
			Row.of("football"),
			Row.of("football"),
			Row.of("basketball"),
			Row.of("basketball"),
			Row.of("tennis")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string");
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0")
			.setStringOrderType("frequency_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		model.print();
	}
}
```

### 运行结果



column_index|token|token_index
------------|-----|-----------
-1|{"selectedCols":"[\"f0\"]","selectedColTypes":"[\"VARCHAR\"]"}|null
0|tennis|0
0|basketball|1
0|football|2
