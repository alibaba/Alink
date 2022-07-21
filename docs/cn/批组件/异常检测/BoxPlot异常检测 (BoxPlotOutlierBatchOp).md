# BoxPlot异常检测 (BoxPlotOutlierBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.BoxPlotOutlierBatchOp

Python 类名：BoxPlotOutlierBatchOp


## 功能介绍
* BoxPlot算法又叫做箱线图算法, 是一种常用的异常检测算法. 
* 它由五个数值点组成：最小值(min)，下四分位数(Q1)，中位数(median)，上四分位数(Q3)，最大值(max)，大于Q3 + K * IQR和小于Q1 - K * IQR的点定义为异常值(Outlier)。
* k通常取值为1.5或者3.


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| direction | 方向 | 检测异常的方向 | String |  | "POSITIVE", "NEGATIVE", "BOTH" | "BOTH" |
| featureCol | 特征列名 | 特征列名，默认选最左边的列 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
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

outlierOp = BoxPlotOutlierBatchOp()\
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

public class BoxPlotOutlierBatchOpTest extends AlinkTestBase {
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

		BatchOperator <?> outlier = new BoxPlotOutlierBatchOp()
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

<div align=left><img src="https://img.alicdn.com/imgextra/i1/O1CN01EZkptl1VPSL0LYmu9_!!6000000002645-2-tps-1250-284.png" height="60%" width="60%"></div>



