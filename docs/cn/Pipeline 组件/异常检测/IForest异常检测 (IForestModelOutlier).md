# IForest异常检测 (IForestModelOutlier)
Java 类名：com.alibaba.alink.pipeline.outlier.IForestModelOutlier

Python 类名：IForestModelOutlier


## 功能介绍
iForest 可以识别数据中异常点，在异常检测领域有比较好的效果。算法使用 sub-sampling 方法，降低了算法的计算复杂度。

### 文献或出处
1. [Isolation Forest](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf?q=isolation-forest)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numTrees | 模型中树的棵数 | 模型中树的棵数 | Integer |  |  | 100 |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| subsamplingSize | 每棵树的样本采样行数 | 每棵树的样本采样行数，默认 256 ，最小 2 ，最大 100000 . | Integer |  | [1, 100000] | 256 |
| tensorCol | tensor列 | tensor列 | String |  | 所选列类型为 [BOOL_TENSOR, BYTE_TENSOR, DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR, STRING, STRING_TENSOR, TENSOR, UBYTE_TENSOR] | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

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

modelOutlier = IForestModelOutlier()\
    .setFeatureCols(["val"])\
    .setOutlierThreshold(3.0)\
    .setPredictionCol("pred")\
    .setPredictionDetailCol("pred_detail")

evalOp = EvalOutlierBatchOp()\
    .setLabelCol("label")\
    .setPredictionDetailCol("pred_detail")\
    .setOutlierValueStrings(["1"])

metrics = modelOutlier\
    .fit(dataOp)\
    .transform(dataOp)\
    .link(evalOp)\
    .collectMetrics()

print(metrics)
```

### Java 代码

```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalOutlierBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import com.alibaba.alink.pipeline.outlier.IForestModelOutlier;
import org.junit.Assert;
import org.junit.Test;

public class IForestModelOutlierTest {
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
			new String[] {"val", "label"});

		IForestModelOutlier iForestModelOutlier = new IForestModelOutlier()
			.setFeatureCols("val")
			.setOutlierThreshold(3.0)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		EvalOutlierBatchOp eval = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("pred_detail")
			.setOutlierValueStrings("1");

		OutlierMetrics metrics = iForestModelOutlier
			.fitAndTransform(data)
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
