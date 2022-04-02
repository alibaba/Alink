# IForest模型异常检测预测 (IForestModelOutlierPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.IForestModelOutlierPredictBatchOp

Python 类名：IForestModelOutlierPredictBatchOp


## 功能介绍
iForest 可以识别数据中异常点，在异常检测领域有比较好的效果。算法使用 sub-sampling 方法，降低了算法的计算复杂度。

### 文献或出处
1. [Isolation Forest](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf?q=isolation-forest)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

## 代码示例

### Python 代码

```python
import pandas as pd
df = pd.DataFrame([
[0.73, 0],
[0.24, 0],
[0.63, 0],
[0.55, 0],
[0.73, 0],
[0.41, 0]
])

dataOp = BatchOperator.fromDataframe(df, schemaStr='val double, label int')

trainOp = IForestModelOutlierTrainBatchOp()\
.setFeatureCols(["val"])

predOp = IForestModelOutlierPredictBatchOp()\
.setOutlierThreshold(3.0)\
.setPredictionCol("pred")\
.setPredictionDetailCol("pred_detail")

predOp.linkFrom(trainOp.linkFrom(dataOp), dataOp)

evalOp = EvalOutlierBatchOp()\
.setLabelCol("label")\
.setPredictionDetailCol("pred_detail")\
.setOutlierValueStrings(["1"]);

metrics = predOp\
.link(evalOp)\
.collectMetrics()

print(metrics)
```

### Java 代码

```java
package com.alibaba.alink.operator.batch.outlier;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalOutlierBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class IForestModelOutlierTrainBatchOpTest extends AlinkTestBase {

	@Test
	public void test() {
		BatchOperator <?> data = new MemSourceBatchOp(
			new Object[][] {
				{0.73, 0},
				{0.24, 0},
				{0.63, 0},
				{0.55, 0},
				{0.73, 0},
				{0.41, 0},
			},
			new String[]{"val", "label"});

		IForestModelOutlierTrainBatchOp trainOp = new IForestModelOutlierTrainBatchOp()
			.setFeatureCols("val");

		IForestModelOutlierPredictBatchOp predOp = new IForestModelOutlierPredictBatchOp()
			.setOutlierThreshold(3.0)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		predOp.linkFrom(trainOp.linkFrom(data), data);

		EvalOutlierBatchOp eval = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("pred_detail")
			.setOutlierValueStrings("1");

		OutlierMetrics metrics = predOp
			.link(eval)
			.collectMetrics();

		Assert.assertEquals(1.0, metrics.getAccuracy(), 10e-6);
	}
}

```

### 运行结果

-------------------------------- Metrics: --------------------------------
Outlier values: [1]		Normal values: [0]
Auc:NaN	Accuracy:1	Precision:1	Recall:0	F1:0
|Pred\Real|Outlier|Normal|
|---------|-------|------|
|  Outlier|      0|     0|
|   Normal|      0|     6|
