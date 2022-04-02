# 局部异常因子异常检测 (LofOutlierBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.LofOutlierBatchOp

Python 类名：LofOutlierBatchOp


## 功能介绍

根据数据样本的局部异常因子值（Local Outlier Factor, LOF）判断样本是否异常。

### 算法原理

LOF 是根据样本点间距离关系计算得到的数值，用$d(\cdot, \cdot)$表示两个样本点的距离。

LOF 的计算过程包含以下几个步骤：

1. 对于样本点 $p$，找到其最近的 $k$ 个样本点（不包含自身），称作 $p$ 的 k-最近邻，记为$N_p$；其中的距离最大值记为样本点 $p$ 的 k-距离： $\textrm{k-distance}(p)=\max_{o\in
   N_p}d(p, o)$；
2. 对于样本点 $p$ 的 k-最近邻，计算每个样本点的到达距离（reach-distance）：$\textrm{reach}(p, o)=\max(\textrm{k-distance}(p),d(p, o))
   $；
3. 定义样本点 $p$ 的局部可达性密度（local reachability density, lrd）为： $\textrm{lrd}_p = 1/(\sum_{o\in N_p} \textrm{reach}(p, o)
   /|N_p|)$；
4. 样本点的 $p$ 的局部异常因子 LOF 可以通过 lrd 来计算：$\textrm{lof}_p = \sum_{o\in N_p}\frac{\textrm{lrd}_o}{\textrm{lrd}_p} / |N_p|$.

需要注意的是，当有大于 $k$ 个样本点具有完全一样的坐标（特征）时，会导致某些点的 lrd 值计算出现除 0 的情况。此时应该在计算中增加个极小的数值来避免出现这种情况。

在判定采样点是否为异常值。原论文建议取 1.5 为阈值， LOF 值大于 1.5 的可以认为是异常点。当然也可以采用其他阈值或者按一定比例进行判定。

### 使用方式

在使用组件时，k-最近邻的 $k$ 值通过参数 numNeighbors 指定，采样点的坐标（特征）可以通过参数 featureCols 或者参数 vectorCol 指定。 通过参数 distanceType
可以指定采样点间的距离计算方式，默认为欧式距离。

在数据量大时，LOF 算法计算速度会比较慢。此时可以通过参数 maxSampleNumPerGroup，将数据分隔为若干组分别进行计算和判断。

在判定采样点是否为异常点时，可以通过设置参数 outlierThreshold 根据阈值判断，也可以根据数异常点数量（参数 maxOutlierNumPerGroup）、比例（参数 maxOutlierRatio）等来判断。

### 文献索引

[LOF: Identifying Density-Based Local Outliers](https://www.dbs.ifi.lmu.de/Publikationen/Papers/LOF.pdf)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN" |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  | null |
| maxOutlierNumPerGroup | 每组最大异常点数目 | 每组最大异常点数目 | Integer |  |  |
| maxOutlierRatio | 最大异常点比例 | 算法检测异常点的最大比例 | Double |  |  |
| maxSampleNumPerGroup | 每组最大样本数目 | 每组最大样本数目 | Integer |  |  |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| tensorCol | tensor列 | tensor列 | String |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| numNeighbors | 相邻点个数 | 构造近邻图使用的相邻点个数 | Integer |  | 5 |
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

outlierOp = LofOutlierBatchOp()\
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
package examples;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalOutlierBatchOp;
import com.alibaba.alink.operator.batch.outlier.LofOutlierBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import org.junit.Test;

public class LofOutlierBatchOpTest {
	@Test
	public void testLofOutlierBatchOp() throws Exception {
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

		BatchOperator <?> outlier = new LofOutlierBatchOp()
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

		System.out.println(metrics);
	}
}
```
### 运行结果
```
-------------------------------- Metrics: --------------------------------
Outlier values: [1]		Normal values: [0]
Auc:NaN	Accuracy:1	Precision:1	Recall:0	F1:0
|Pred\Real|Outlier|Normal|
|---------|-------|------|
|  Outlier|      0|     0|
|   Normal|      0|     6|
```
