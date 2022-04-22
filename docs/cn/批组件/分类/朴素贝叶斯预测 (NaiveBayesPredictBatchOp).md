# 朴素贝叶斯预测 (NaiveBayesPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.NaiveBayesPredictBatchOp

Python 类名：NaiveBayesPredictBatchOp


## 功能介绍

使用朴素贝叶斯模型用于多分类任务的预测。

### 使用方式

该组件是预测组件，需要配合训练组件 NaiveBayesTrainBatchOp 使用。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
       [1.0, 1.0, 0.0, 1.0, 1],
       [1.0, 0.0, 1.0, 1.0, 1],
       [1.0, 0.0, 1.0, 1.0, 1],
       [0.0, 1.0, 1.0, 0.0, 0],
       [0.0, 1.0, 1.0, 0.0, 0],
       [0.0, 1.0, 1.0, 0.0, 0],
       [0.0, 1.0, 1.0, 0.0, 0],
       [1.0, 1.0, 1.0, 1.0, 1],
       [0.0, 1.0, 1.0, 0.0, 0]
])

batchData = BatchOperator.fromDataframe(df_data, schemaStr='f0 double, f1 double, f2 double, f3 double, label int')

colnames = ["f0","f1","f2", "f3"]
ns = NaiveBayesTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(ns)

predictor = NaiveBayesPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesPredictBatchOpTest {
	@Test
	public void testNaiveBayesPredictBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1.0, 1.0, 0.0, 1.0, 1),
			Row.of(1.0, 0.0, 1.0, 1.0, 1),
			Row.of(1.0, 0.0, 1.0, 1.0, 1),
			Row.of(0.0, 1.0, 1.0, 0.0, 0),
			Row.of(0.0, 1.0, 1.0, 0.0, 0),
			Row.of(0.0, 1.0, 1.0, 0.0, 0),
			Row.of(0.0, 1.0, 1.0, 0.0, 0),
			Row.of(1.0, 1.0, 1.0, 1.0, 1),
			Row.of(0.0, 1.0, 1.0, 0.0, 0)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df_data,
			"f0 double, f1 double, f2 double, f3 double, label int");
		BatchOperator <?> ns = new NaiveBayesTrainBatchOp().setFeatureCols("f0", "f1", "f2", "f3").setLabelCol(
			"label");
		BatchOperator model = batchData.link(ns);
		BatchOperator <?> predictor = new NaiveBayesPredictBatchOp().setPredictionCol("pred");
		predictor.linkFrom(model, batchData).print();
	}
}
```
### 运行结果

| f0  | f1  | f2  | f3  | label | pred |
|-----|-----|-----|-----|-------|------|
| 1.0 | 1.0 | 0.0 | 1.0 | 1     | 1    |
| 1.0 | 0.0 | 1.0 | 1.0 | 1     | 1    |
| 1.0 | 0.0 | 1.0 | 1.0 | 1     | 1    |
| 0.0 | 1.0 | 1.0 | 0.0 | 0     | 0    |
| 0.0 | 1.0 | 1.0 | 0.0 | 0     | 0    |
| 0.0 | 1.0 | 1.0 | 0.0 | 0     | 0    |
| 0.0 | 1.0 | 1.0 | 0.0 | 0     | 0    |
| 1.0 | 1.0 | 1.0 | 1.0 | 1     | 1    |
| 0.0 | 1.0 | 1.0 | 0.0 | 0     | 0    |
