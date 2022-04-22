# MultiStringIndexer预测 (MultiStringIndexerPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.MultiStringIndexerPredictStreamOp

Python 类名：MultiStringIndexerPredictStreamOp


## 功能介绍
多列字符串转换组件，输入的模型数据来自 MultiStringIndexerTrainBatchOp 组件的输出，训练的时候指定多个列，每个列单独编码。
这个组件为流式预测组件，预测时需要指定列名，列名必须与训练时列名相同。如果转换时指定了训练时不存在的列名，会报异常。
支持按照一定的次序编码。如随机、出现频次生序，出现频次降序、字符串生序、字符串降序5种方式。
设置 setStringOrderType 参数时分别对应 random frequency_asc frequency_desc alphabet_asc alphabet_desc。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP", "ERROR", "SKIP" | "KEEP" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  |  | null |
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
    ["football", "apple"],
    ["football", "banana"],
    ["football", "banana"],
    ["basketball", "orange"],
    ["basketball", "grape"],
    ["tennis", "grape"],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='f0 string,f1 string')

stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f0 string,f1 string')

stringindexer = MultiStringIndexerTrainBatchOp() \
     .setSelectedCols(["f0", "f1"]) \
     .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)

predictor = MultiStringIndexerPredictStreamOp(model)\
     .setSelectedCols(["f0", "f1"])\
     .setOutputCols(["f0_indexed", "f1_indexed"])

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
			Row.of("football", "apple"),
			Row.of("football", "banana"),
			Row.of("football", "banana"),
			Row.of("basketball", "orange"),
			Row.of("basketball", "grape"),
			Row.of("tennis", "grape")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "f0 string,f1 string");
		StreamOperator <?> stream_data = new MemSourceStreamOp(df_data, "f0 string,f1 string");
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.setStringOrderType("frequency_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		StreamOperator <?> predictor = new MultiStringIndexerPredictStreamOp(model)
			.setSelectedCols("f0", "f1")
			.setOutputCols("f0_indexed", "f1_indexed");
		predictor.linkFrom(stream_data).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

f0|f1|f0_indexed|f1_indexed
---|---|----------|----------
basketball|orange|1|0
football|apple|2|1
tennis|grape|0|3
football|banana|2|2
basketball|grape|1|3
football|banana|2|2
