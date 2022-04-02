# 朴素贝叶斯文本分类预测 (NaiveBayesTextPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.NaiveBayesTextPredictBatchOp

Python 类名：NaiveBayesTextPredictBatchOp


## 功能介绍

训练一个朴素贝叶斯文本分类模型用于多分类任务。

### 使用方式

该组件是预测组件，需要配合预测组件 NaiveBayesTextTrainBatchOp 使用。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
          ["$31$0:1.0 1:1.0 2:1.0 30:1.0","1.0  1.0  1.0  1.0", '1'],
          ["$31$0:1.0 1:1.0 2:0.0 30:1.0","1.0  1.0  0.0  1.0", '1'],
          ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
          ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
          ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
          ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
          ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0']
])

batchData = BatchOperator.fromDataframe(df_data, schemaStr='sv string, dv string, label string')

# train op
ns = NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label")
model = batchData.link(ns)
# predict op
predictor = NaiveBayesTextPredictBatchOp().setVectorCol("sv").setReservedCols(["sv", "label"]).setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesTextPredictBatchOpTest {
	@Test
	public void testNaiveBayesTextPredictBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", "1"),
			Row.of("$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", "1"),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", "1"),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", "1"),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "0"),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "0"),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "0")
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df_data, "sv string, dv string, label string");
		BatchOperator <?> ns = new NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label");
		BatchOperator model = batchData.link(ns);
		BatchOperator <?> predictor = new NaiveBayesTextPredictBatchOp().setVectorCol("sv").setReservedCols("sv",
			"label").setPredictionCol("pred");
		predictor.linkFrom(model, batchData).print();
	}
}
```
### 运行结果

| sv                             | label | pred |
|--------------------------------|-------|------|
| "$31$0:1.0 1:1.0 2:1.0 30:1.0" | 1     | 1    |
| "$31$0:1.0 1:1.0 2:0.0 30:1.0" | 1     | 1    |
| "$31$0:1.0 1:0.0 2:1.0 30:1.0" | 1     | 1    |
| "$31$0:1.0 1:0.0 2:1.0 30:1.0" | 1     | 1    |
| "$31$0:0.0 1:1.0 2:1.0 30:0.0" | 0     | 0    |
| "$31$0:0.0 1:1.0 2:1.0 30:0.0" | 0     | 0    |
| "$31$0:0.0 1:1.0 2:1.0 30:0.0" | 0     | 0    |
