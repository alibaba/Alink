# One Class SVM异常检测模型预测 (OcsvmModelOutlierPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.OcsvmModelOutlierPredictBatchOp

Python 类名：OcsvmModelOutlierPredictBatchOp


## 功能介绍
* 与传统SVM不同的是，one-class SVM是一种非监督的学习算法，经常被用来做异常点检测。在该算法的训练集中只有一类positive（或者negative）的数据，而没有（或存在极少量）另外一类，通常称其为异常点。该算法需要学习（learn）的就是边界（boundary），而不是最大间隔（maximum margin），通过边界对异常点进行预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |



## 代码示例

### Python 代码
```python
data = RandomTableSourceBatchOp()\
    .setNumCols(5)\
    .setNumRows(1000)\
    .setIdCol("id")\
    .setOutputCols(["x1", "x2", "x3", "x4"])

dataTest = data
ocsvm = OcsvmModelOutlierTrainBatchOp().setFeatureCols(["x1", "x2", "x3", "x4"]).setGamma(0.5).setNu(0.1).setKernelType("RBF")
model = data.link(ocsvm)
predictor = OcsvmModelOutlierPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, dataTest).print()
```

### java 代码
```java
package com.alibaba.alink.operator.batch.outlier;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import org.junit.Test;

public class OneClassSvmTrainBatchOpTest {

	@Test
	public void testPipelineTable() throws Exception {
		BatchOperator <?> data = new RandomTableSourceBatchOp()
			.setNumCols(5)
			.setNumRows(1000L)
			.setIdCol("id")
			.setOutputCols("x1", "x2", "x3", "x4");

		OcsvmModelOutlierTrainBatchOp model = new OcsvmModelOutlierTrainBatchOp()
			.setFeatureCols(new String[] {"x1", "x2", "x3", "x4"})
			.setGamma(0.5)
			.setNu(0.1)
			.setKernelType("RBF").linkFrom(data);
		new OcsvmModelOutlierPredictBatchOp().setPredictionCol("pred").linkFrom(model, data).print();
	}
}
```

### 运行结果
id | x1     | x2     | x3   | x4     |pred
---|--------|--------|------|--------|----
0| 0.7310 | 0.2405 | 0.6374 | 0.5504 |false
12| 0.5975 | 0.3332 | 0.3852 | 0.9848 |false
24| 0.8792 | 0.9412 | 0.2750 | 0.1289 |true
36| 0.1466 | 0.0232 | 0.5467 | 0.9645 |true
48| 0.1045 | 0.6251 | 0.4108 | 0.7763 |false
60| 0.9907 | 0.4872 | 0.7462 | 0.7332 |false
...| ...    | ...    | ...  | ...    |...
997| 0.1339 | 0.0831 | 0.9786 | 0.7224 |true
998| 0.7150 | 0.1432 | 0.4630 | 0.0045 |false
999| 0.0715 | 0.3484 | 0.3388 | 0.8594 |false
