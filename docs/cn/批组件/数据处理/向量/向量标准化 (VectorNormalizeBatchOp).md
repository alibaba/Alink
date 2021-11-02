# 向量标准化 (VectorNormalizeBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.vector.VectorNormalizeBatchOp

Python 类名：VectorNormalizeBatchOp


## 功能介绍
对 Vector 进行正则化操作。

指定参数范数的阶，例如p = 2, 对于向量<x1, x2, x3>，计算向量的平方和再开二次方记为norm，最终计算结果为<x1/norm, x2/norm, x3/norm>

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| p | 范数的阶 | 范数的阶，默认2 | Double |  | 2.0 |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["1:3,2:4,4:7", 1],
    ["0:3,5:5", 3],
    ["2:4,4:5", 4]
])

data = BatchOperator.fromDataframe(df, schemaStr="vec string, id bigint")
VectorNormalizeBatchOp().setSelectedCol("vec").setOutputCol("vec_norm").linkFrom(data).collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorNormalizeBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorNormalizeBatchOpTest {
	@Test
	public void testVectorNormalizeBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1:3,2:4,4:7", 1),
			Row.of("0:3,5:5", 3),
			Row.of("2:4,4:5", 4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec string, id int");
		new VectorNormalizeBatchOp().setSelectedCol("vec").setOutputCol("vec_norm").linkFrom(data).print();
	}
}
```
### 运行结果


| vec         | id   | vec_norm                                 |
| ----------- | ---- | ---------------------------------------- |
| 1:3,2:4,4:7 | 1    | 1:0.34874291623145787 2:0.46499055497527714 4:0.813733471206735 |
| 0:3,5:5     | 3    | 0:0.5144957554275265 5:0.8574929257125441 |
| 2:4,4:5     | 4    | 2:0.6246950475544243 4:0.7808688094430304 |
