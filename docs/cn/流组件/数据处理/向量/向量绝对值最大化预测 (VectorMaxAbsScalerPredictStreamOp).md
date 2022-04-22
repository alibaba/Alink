# 向量绝对值最大化预测 (VectorMaxAbsScalerPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.vector.VectorMaxAbsScalerPredictStreamOp

Python 类名：VectorMaxAbsScalerPredictStreamOp


## 功能介绍

vector绝对值最大标准化是对vector数据按照最大值和最小值进行标准化的组件, 将数据归一到-1和1之间。

预测组件使用VectorMaxAbsScalerTrainBatchOp训练生成的模型，处理数据之后生成结果数据。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
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

df = pd.DataFrame([
    ["a", "10.0, 100"],
    ["b", "-2.5, 9"],
    ["c", "100.2, 1"],
    ["d", "-99.9, 100"],
    ["a", "1.4, 1"],
    ["b", "-2.2, 9"],
    ["c", "100.9, 1"]
])
data = BatchOperator.fromDataframe(df, schemaStr="col string, vec string")
dataStream = StreamOperator.fromDataframe(df, schemaStr="col string, vec string")

trainOp = VectorMaxAbsScalerTrainBatchOp()\
           .setSelectedCol("vec")
model = trainOp.linkFrom(data)

streamPredictOp = VectorMaxAbsScalerPredictStreamOp(model)
streamPredictOp.linkFrom(dataStream).print()

StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorMaxAbsScalerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorMaxAbsScalerPredictStreamOpTest {
	@Test
	public void testVectorMaxAbsScalerPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", "10.0, 100"),
			Row.of("b", "-2.5, 9"),
			Row.of("c", "100.2, 1"),
			Row.of("d", "-99.9, 100"),
			Row.of("a", "1.4, 1"),
			Row.of("b", "-2.2, 9"),
			Row.of("c", "100.9, 1")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "col string, vec string");
		StreamOperator <?> dataStream = new MemSourceStreamOp(df, "col string, vec string");
		BatchOperator <?> trainOp = new VectorMaxAbsScalerTrainBatchOp()
			.setSelectedCol("vec");
		BatchOperator <?> model = trainOp.linkFrom(data);
		StreamOperator <?> streamPredictOp = new VectorMaxAbsScalerPredictStreamOp(model);
		streamPredictOp.linkFrom(dataStream).print();
		StreamOperator.execute();
	}
}
```
### 运行结果

col1|vec
----|---
c|1.0,0.01
b|-0.024777006937561942,0.09
d|-0.9900891972249752,1.0
a|0.09910802775024777,1.0
b|-0.02180376610505451,0.09
c|0.9930624380574826,0.01
a|0.013875123885034686,0.01
