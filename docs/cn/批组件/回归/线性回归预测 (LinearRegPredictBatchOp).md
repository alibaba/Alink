# 线性回归预测 (LinearRegPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.LinearRegPredictBatchOp

Python 类名：LinearRegPredictBatchOp


## 功能介绍
* 线性回归是一个回归算法
* 线性回归组件支持稀疏、稠密两种数据格式
* 线性回归组件支持带样本权重的训练


## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]
])

batchData = BatchOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')

lr = LinearRegTrainBatchOp()\
        .setFeatureCols(["f0","f1"])\
        .setLabelCol("label")

model = batchData.link(lr)

predictor = LinearRegPredictBatchOp()\
    .setPredictionCol("pred")

predictor.linkFrom(model, batchData).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LinearRegPredictBatchOpTest {
	@Test
	public void testLinearRegPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1),
			Row.of(5, 3, 3)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "f0 int, f1 int, label int");
		BatchOperator <?> lr = new LinearRegTrainBatchOp()
			.setFeatureCols("f0", "f1")
			.setLabelCol("label");
		BatchOperator model = batchData.link(lr);
		BatchOperator <?> predictor = new LinearRegPredictBatchOp()
			.setPredictionCol("pred");
		predictor.linkFrom(model, batchData).print();
	}
}
```

### 运行结果
f0 | f1 | label | pred
---|----|-------|-----
   2 |  1   |   1  | 1.000014
   3 |  2   |   1  | 1.538474
   4 |  3   |   2  | 2.076934
   2 |  4   |   1  | 1.138446
   2 |  2   |   1  | 1.046158
   4 |  3   |   2  | 2.076934
   1 |  2   |   1  | 0.553842
   5 |  3   |   3  | 2.569250




