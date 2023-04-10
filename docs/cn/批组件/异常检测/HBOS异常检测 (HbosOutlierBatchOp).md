# HBOS异常检测 (HbosOutlierBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.HbosOutlierBatchOp

Python 类名：HbosOutlierBatchOp


## 功能介绍

Histogram-based Outlier Score 使用直方图统计结果，描述异常值，算法较为简单，上手方便。

### 文献或出处
1. [HBOS](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.401.5686&rep=rep1&type=pdf)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| k | K | 直方图 bin 的数量 | Integer |  | x >= 1 | 10 |
| maxOutlierNumPerGroup | 每组最大异常点数目 | 每组最大异常点数目 | Integer |  |  |  |
| maxOutlierRatio | 最大异常点比例 | 算法检测异常点的最大比例 | Double |  |  |  |
| maxSampleNumPerGroup | 每组最大样本数目 | 每组最大样本数目 | Integer |  |  |  |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| tensorCol | tensor列 | tensor列 | String |  | 所选列类型为 [BOOL_TENSOR, BYTE_TENSOR, DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR, STRING, STRING_TENSOR, TENSOR, UBYTE_TENSOR] | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

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

outlierOp = HbosOutlierBatchOp()\
    .setFeatureCols(["val"])\
    .setOutlierThreshold(3.0)\
    .setPredictionCol("pred")\
    .setPredictionDetailCol("pred_detail")

evalOp = EvalOutlierBatchOp()\
    .setLabelCol("label")\
    .setPredictionDetailCol("pred_detail")\
    .setOutlierValueStrings(["1"])

metrics = dataOp\
    .link(outlierOp)\
    .link(evalOp)\
    .collectMetrics()

print(metrics)
```

### Java 代码

```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalOutlierBatchOp;
import com.alibaba.alink.operator.batch.outlier.HbosOutlierBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import org.junit.Assert;
import org.junit.Test;

public class HbosOutlierBatchOpTest {

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
			new String[] {"val", "label"});

		BatchOperator <?> outlier = new HbosOutlierBatchOp()
			.setFeatureCols("val")
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

-------------------------------- Metrics: --------------------------------
Outlier values: [1]		Normal values: [0]
Auc:NaN	Accuracy:1	Precision:1	Recall:0	F1:0
|Pred\Real|Outlier|Normal|
|---------|-------|------|
|  Outlier|      0|     0|
|   Normal|      0|     6|
