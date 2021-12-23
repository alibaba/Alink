# MultiStringIndexer预测 (MultiStringIndexerPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.MultiStringIndexerPredictStreamOp

Python 类名：MultiStringIndexerPredictStreamOp


## 功能介绍
基于StringIndexer模型，将一列字符串映射为整数。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP" |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |


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

stringindexer = MultiStringIndexerTrainBatchOp() \
     .setSelectedCols(["f0"]) \
     .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)

predictor = MultiStringIndexerPredictStreamOp(model)\
     .setSelectedCols(["f0"])\
     .setOutputCols(["f0_indexed"])

predictor.linkFrom(stream_data).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.MultiStringIndexerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MultiStringIndexerPredictStreamOpTest {
	@Test
	public void testMultiStringIndexerPredictStreamOp() throws Exception {
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
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0")
			.setStringOrderType("frequency_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		StreamOperator <?> predictor = new MultiStringIndexerPredictStreamOp(model)
			.setSelectedCols("f0")
			.setOutputCols("f0_indexed");
		predictor.linkFrom(stream_data).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

f0|f0_indexed
---|----------
football|2
tennis|0
football|2
basketball|1
basketball|1
football|2
