# MultiStringIndexer预测 (MultiStringIndexerPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerPredictBatchOp

Python 类名：MultiStringIndexerPredictBatchOp


## 功能介绍
基于MultiStringIndexer模型，将多列字符串映射为整数。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP" |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


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

predictor = MultiStringIndexerPredictBatchOp().setSelectedCols(["f0"]).setOutputCols(["f0_indexed"])

model = stringindexer.linkFrom(data)
predictor.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MultiStringIndexerPredictBatchOpTest {
	@Test
	public void testMultiStringIndexerPredictBatchOp() throws Exception {
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
		BatchOperator <?> predictor = new MultiStringIndexerPredictBatchOp().setSelectedCols("f0").setOutputCols(
			"f0_indexed");
		BatchOperator model = stringindexer.linkFrom(data);
		predictor.linkFrom(model, data).print();
	}
}
```

### 运行结果

f0|f0_indexed
---|---
football|2
football|2
football|2
basketball|1
basketball|1
tennis|0
