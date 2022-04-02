# 向量标准化训练 (VectorStandardScalerTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerTrainBatchOp

Python 类名：VectorStandardScalerTrainBatchOp


## 功能介绍

 标准化是对向量数据进行按正态化处理的组件

VectorStandardScalerTrainBatchOp 计算向量的每一列的均值和方差，组件可以指定默认均值为0，标准差为1。
生成向量标准化的模型，在 VectorStandardScalerPredictBatchOp 中加载，对数据做标准化处理。

输入的向量可以同时包含稀疏向量和稠密向量，向量维度也可以不相同。输入稠密向量维度不够时，没有的维度默认为0。
 
## 参数说明 

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| withMean | 是否使用均值 | 是否使用均值，默认使用 | Boolean |  | true |
| withStd | 是否使用标准差 | 是否使用标准差，默认使用 | Boolean |  | true |


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

data = BatchOperator.fromDataframe(df, schemaStr="col string, vector string")
trainOp = VectorStandardScalerTrainBatchOp().setSelectedCol("vector")
model = trainOp.linkFrom(data)
VectorStandardScalerPredictBatchOp().linkFrom(model, data).collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorStandardScalerTrainBatchOpTest {
	@Test
	public void testVectorStandardScalerTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", "10.0, 100"),
			Row.of("b", "-2.5, 9"),
			Row.of("c", "100.2, 1"),
			Row.of("d", "-99.9, 100"),
			Row.of("a", "1.4, 1"),
			Row.of("b", "-2.2, 9"),
			Row.of("c", "100.9, 1")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "col string, vector string");
		BatchOperator <?> trainOp = new VectorStandardScalerTrainBatchOp().setSelectedCol("vector");
		BatchOperator <?> model = trainOp.linkFrom(data);
		new VectorStandardScalerPredictBatchOp().linkFrom(model, data).print();
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

