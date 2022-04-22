# StringIndexer预测 (StringIndexerPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.StringIndexerPredictStreamOp

Python 类名：StringIndexerPredictStreamOp


## 功能介绍
基于StringIndexer模型，将一列字符串映射为整数。该组件为流式组件。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP", "ERROR", "SKIP" | "KEEP" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ["football"],
    ["football"],
    ["football"],
    ["basketball"],
    ["basketball"],
    ["tennis"],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='f0 string')

stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f0 string')

stringindexer = StringIndexerTrainBatchOp() \
    .setSelectedCol("f0") \
    .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)

predictor = StringIndexerPredictStreamOp(model)\
                .setSelectedCol("f0")\
                .setOutputCol("f0_indexed")

predictor.linkFrom(stream_data).print()

StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.StringIndexerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StringIndexerPredictStreamOpTest {
	@Test
	public void testStringIndexerPredictStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("football"),
			Row.of("football"),
			Row.of("football"),
			Row.of("basketball"),
			Row.of("basketball"),
			Row.of("tennis")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "f0 string");
		StreamOperator <?> stream_data = new MemSourceStreamOp(df_data, "f0 string");
		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setStringOrderType("frequency_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		StreamOperator <?> predictor = new StringIndexerPredictStreamOp(model)
			.setSelectedCol("f0")
			.setOutputCol("f0_indexed");
		predictor.linkFrom(stream_data).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

f0|f0_indexed
---|----------
basketball|1
football|2
tennis|0
basketball|1
football|2
football|2
