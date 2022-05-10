# 局部核密度估计异常检测 (KdeOutlierBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.KdeOutlierBatchOp

Python 类名：KdeOutlierBatchOp


## 功能介绍

KDE（Kernel Density Estimation核密度估计）是一种通过数据样本集，得到总体的概率分布的非参数估计方法。KDE异常检测算法将概率密度小的点视为异常点。

### 算法原理

该组件以每个点的数据、带宽作为参数，根据设置的核函数（高斯核或线性核）估计样本中每个数据点及其附近的概率密度函数。

- 带宽(bandwidth)：带宽设的越小，误差越小，但方差越大，KDE整体曲线就越陡峭，反之，就越平坦。不同的带宽对拟合结果的影响可能很大。
- 核函数(kernel)：用来对每个数据点得到光滑的、积分为1的概率密度估计。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| bandwidth | KDE带宽 | 核密度函数带宽参数 | Double | ✓ | [0.0, +inf) |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN", "COSINE", "INNERPRODUCT", "CITYBLOCK", "JACCARD", "PEARSON" | "EUCLIDEAN" |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| kernelType | 核密度函数类型 | 核密度函数类型，可取为"GAUSSIAN"，"LINEAR" | String |  | "GAUSSIAN", "LINEAR" | "GAUSSIAN" |
| maxOutlierNumPerGroup | 每组最大异常点数目 | 每组最大异常点数目 | Integer |  |  |  |
| maxOutlierRatio | 最大异常点比例 | 算法检测异常点的最大比例 | Double |  |  |  |
| maxSampleNumPerGroup | 每组最大样本数目 | 每组最大样本数目 | Integer |  |  |  |
| numNeighbors | 相邻点个数 | 计算KDE时使用的相邻点个数(默认使用全部点) | Integer |  |  | -1 |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| tensorCol | tensor列 | tensor列 | String |  | 所选列类型为 [BOOL_TENSOR, BYTE_TENSOR, DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR, STRING, STRING_TENSOR, TENSOR, UBYTE_TENSOR] | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例


### Python 代码

``` python
import pandas as pd
df = pd.DataFrame([
                [-1.1],
                [0.2],
                [101.1],
                [0.3]
        ])

dataOp = BatchOperator.fromDataframe(df, schemaStr='val double')

outlierOp = KdeOutlierBatchOp()\
			.setFeatureCols(["val"])\
			.setBandwidth(4.0)\
			.setOutlierThreshold(15.0)\
			.setPredictionCol("pred")\
			.setPredictionDetailCol("pred_detail")

dataOp.link(outlierOp).print()
```

### Java 代码

``` java 
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.outlier.KdeOutlierBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

public class KdeOutlierBatchOpTest {

	@Test
	public void testBatchOp() throws Exception {
		BatchOperator <?> data = new MemSourceBatchOp(
			new Object[][] {
				{-1.1},
				{0.2},
				{101.1},
				{0.3}}
			, new String[] {"val"}
		);

		KdeOutlierBatchOp kdeOutlierBatchOp = new KdeOutlierBatchOp()
			.setBandwidth(4.)
			.setOutlierThreshold(15.)
			.setFeatureCols("val")
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail")
			.linkFrom(data)
			.print();
	}
}
```

### 运行结果

| val |pred| pred_detail                                                                                  |
|-----|----|------------------------------------------------------------------------------------------|
| -1.1 |false| {"outlier_score":"13.881629239608612","KDE":"0.0720376537032619","is_outlier":"false"}   |
| 0.2 |false| {"outlier_score":"13.603363005188747","KDE":"0.07351123392197714","is_outlier":"false"}  |
| 101.1 |true| {"outlier_score":"40.106052394096","KDE":"0.02493389252508955","is_outlier":"true"}      |
| 0.3 |false| {"outlier_score":"13.640235707107605","KDE":"0.07331251610842206","is_outlier":"false"}  |
