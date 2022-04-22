# 向量绝对值最大化训练 (VectorMaxAbsScalerTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerTrainBatchOp

Python 类名：VectorMaxAbsScalerTrainBatchOp


## 功能介绍

 vector绝对值最大标准化是对vector数据按照数值最大绝对值进行标准化的组件, 将数据归一到-1和1之间。输入的向量维度可以不相同。

计算公式为 value / max( | value | )

该组件生成Vector绝对值最大标准化的模型

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |



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
trainOp = VectorMaxAbsScalerTrainBatchOp()\
           .setSelectedCol("vec")
model = trainOp.linkFrom(data) 

batchPredictOp = VectorMaxAbsScalerPredictBatchOp()
batchPredictOp.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorMaxAbsScalerTrainBatchOpTest {
	@Test
	public void testVectorMaxAbsScalerTrainBatchOp() throws Exception {
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
		BatchOperator <?> trainOp = new VectorMaxAbsScalerTrainBatchOp()
			.setSelectedCol("vec");
		BatchOperator <?> model = trainOp.linkFrom(data);
		BatchOperator <?> batchPredictOp = new VectorMaxAbsScalerPredictBatchOp();
		batchPredictOp.linkFrom(model, data).print();
	}
}
```
### 运行结果

col|vec
---|---
a|0.09910802775024777 1.0
b|-0.024777006937561942 0.09
c|0.9930624380574826 0.01
d|-0.9900891972249752 1.0
a|0.013875123885034686 0.01
b|-0.02180376610505451 0.09
c|1.0 0.01


