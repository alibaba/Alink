# 朴素贝叶斯预测 (NaiveBayesPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.classification.NaiveBayesPredictStreamOp

Python 类名：NaiveBayesPredictStreamOp


## 功能介绍

使用朴素贝叶斯模型用于多分类任务的预测。

### 使用方式

该组件是预测组件，需要配合训练组件 NaiveBayesTrainBatchOp 使用。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
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

# stream data
streamData = StreamOperator.fromDataframe(df_data, schemaStr='f0 double, f1 double, f2 double, f3 double, label int')

colnames = ["f0","f1","f2", "f3"]
ns = NaiveBayesTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(ns)

predictor = NaiveBayesPredictStreamOp(model).setPredictionCol("pred")
predictor.linkFrom(streamData).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.NaiveBayesPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesPredictStreamOpTest {
	@Test
	public void testNaiveBayesPredictStreamOp() throws Exception {
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
		StreamOperator <?> streamData = new MemSourceStreamOp(df_data,
			"f0 double, f1 double, f2 double, f3 double, label int");
		String[] colnames = new String[] {"f0", "f1", "f2", "f3"};
		BatchOperator <?> ns = new NaiveBayesTrainBatchOp().setFeatureCols(colnames).setLabelCol("label");
		BatchOperator <?> model = batchData.link(ns);
		StreamOperator <?> predictor = new NaiveBayesPredictStreamOp(model).setPredictionCol("pred");
		predictor.linkFrom(streamData).print();
		StreamOperator.execute();
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
