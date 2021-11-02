# 朴素贝叶斯 (NaiveBayes)
Java 类名：com.alibaba.alink.pipeline.classification.NaiveBayes

Python 类名：NaiveBayes


## 功能介绍

* 朴素贝叶斯分类器是一个多分类算法
* 朴素贝叶斯分类器组件支持稀疏、稠密两种数据格式
* 朴素贝叶斯分类器组件支持带样本权重的训练
* 数据特征列可以是离散的也可以是连续的。对于连续特征，朴素贝叶斯算法将使用高斯模型进行计算。
* bigint，int等类型默认认为是连续特征。如果想将其按照离散特征来处理，则可以将特征列名写在categoricalCols中。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| smoothing | 算法参数 | 光滑因子，默认为0.0 | Double |  | 0.0 |
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

colnames = ["f0","f1","f2", "f3"]
ns = NaiveBayesTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(ns)

predictor = NaiveBayesPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()
colnames = ["f0","f1","f2", "f3"]
# pipeline model
ns = NaiveBayes().setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
model = ns.fit(batchData)
model.transform(batchData).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.classification.NaiveBayes;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesTest {
	@Test
	public void testNaiveBayes() throws Exception {
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
		NaiveBayes ns = new NaiveBayes()
			.setFeatureCols("f0", "f1", "f2", "f3")
			.setLabelCol("label")
			.setPredictionCol("pred");

		ns.fit(batchData)
			.transform(batchData)
			.print();
	}
}
```
### 运行结果

f0 | f1 | f2 | f3 | label | pred
---|----|----|----|-------|----
1.0|1.0|0.0|1.0|1|1
1.0|0.0|1.0|1.0|1|1
1.0|0.0|1.0|1.0|1|1
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
1.0|1.0|1.0|1.0|1|1
0.0|1.0|1.0|0.0|0|0



