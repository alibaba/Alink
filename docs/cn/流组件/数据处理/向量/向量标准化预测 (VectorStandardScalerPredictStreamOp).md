# 向量标准化预测 (VectorStandardScalerPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.vector.VectorStandardScalerPredictStreamOp

Python 类名：VectorStandardScalerPredictStreamOp


## 功能介绍

标准化是对向量数据进行按正态化处理的组件

加载VectorStandardScalerTrainBatchOp中生成的模型，对向量数据做标准化预处理。
 
## 参数说明 

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


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

data = BatchOperator.fromDataframe(df, schemaStr="col1 string, vec string")
colnames = ["col1", "vec"]
selectedColName = "vec"

trainOp = VectorStandardScalerTrainBatchOp()\
           .setSelectedCol(selectedColName)

model = trainOp.linkFrom(data)

#batch predict
batchPredictOp = VectorStandardScalerPredictBatchOp()
batchPredictOp.linkFrom(model, data).print()

#stream predict
streamData = StreamOperator.fromDataframe(df, schemaStr="col1 string, vec string")

streamPredictOp = VectorStandardScalerPredictStreamOp(trainOp)
streamData.link(streamPredictOp).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorStandardScalerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorStandardScalerPredictStreamOpTest {
	@Test
	public void testVectorStandardScalerPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", "10.0, 100"),
			Row.of("b", "-2.5, 9"),
			Row.of("c", "100.2, 1"),
			Row.of("d", "-99.9, 100"),
			Row.of("a", "1.4, 1"),
			Row.of("b", "-2.2, 9"),
			Row.of("c", "100.9, 1")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "col1 string, vec string");
		BatchOperator <?> trainOp = new VectorStandardScalerTrainBatchOp()
			.setSelectedCol("vec");
		BatchOperator <?> model = trainOp.linkFrom(data);
		BatchOperator <?> batchPredictOp = new VectorStandardScalerPredictBatchOp();
		batchPredictOp.linkFrom(model, data).print();
		StreamOperator <?> streamData = new MemSourceStreamOp(df, "col1 string, vec string");
		StreamOperator <?> streamPredictOp = new VectorStandardScalerPredictStreamOp(trainOp);
		streamData.link(streamPredictOp).print();
		StreamOperator.execute();
	}
}
```
### 运行结果

col1|vec
----|---
a|-0.07835182408093559,1.4595814453461897
c|1.2269606224811418,-0.6520885789229323
b|-0.2549018445693762,-0.4814485769617911
a|-0.20280511721213143,-0.6520885789229323
c|1.237090541689495,-0.6520885789229323
b|-0.25924323851581327,-0.4814485769617911
d|-1.6687491397923802,1.4595814453461897


