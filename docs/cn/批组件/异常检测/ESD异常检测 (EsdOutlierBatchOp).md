# ESD异常检测 (EsdOutlierBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.EsdOutlierBatchOp

Python 类名：EsdOutlierBatchOp


## 功能介绍
ESD算法是一种常用的异常检测算法.


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| alpha | 置信度 | 置信度 | Double |  |  | 0.05 |
| direction | 方向 | 检测异常的方向 | String |  | "POSITIVE", "NEGATIVE", "BOTH" | "BOTH" |
| featureCol | 特征列名 | 特征列名，默认选最左边的列 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| maxIter | 最大迭代步数 | 最大迭代步数 | Integer |  |  |  |
| maxOutlierNumPerGroup | 每组最大异常点数目 | 每组最大异常点数目 | Integer |  |  |  |
| maxOutlierRatio | 最大异常点比例 | 算法检测异常点的最大比例 | Double |  |  |  |
| maxSampleNumPerGroup | 每组最大样本数目 | 每组最大样本数目 | Integer |  |  |  |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

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

outlierOp = EsdOutlierBatchOp()\
			.setFeatureCol("val")\
			.setOutlierThreshold(3.0)\
			.setPredictionCol("pred")\
			.setPredictionDetailCol("pred_detail")

evalOp = EvalOutlierBatchOp()\
			.setLabelCol("label")\
			.setPredictionDetailCol("pred_detail")\
			.setOutlierValueStrings(["1"]);

metrics = dataOp\
			.link(outlierOp)\
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

public class EsdOutlierBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
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

		BatchOperator <?> outlier = new EsdOutlierBatchOp()
			.setFeatureCol("val")
			.setOutlierThreshold(3.0)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		EvalOutlierBatchOp eval = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("pred_detail")
			.setOutlierValueStrings("1");

		OutlierMetrics metrics = data
			.link(outlier)
			.link(eval)
			.collectMetrics();

		Assert.assertEquals(1.0, metrics.getAccuracy(), 10e-6);

	}
}
```

### 运行结果

无


