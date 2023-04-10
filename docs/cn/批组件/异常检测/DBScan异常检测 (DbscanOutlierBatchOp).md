# DBSCAN异常检测 (DbscanOutlierBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.DbscanOutlierBatchOp

Python 类名：DbscanOutlierBatchOp


## 功能介绍

[DBSCAN](https://en.wikipedia.org/wiki/DBSCAN)，Density-Based Spatial Clustering of Applications with
Noise，是一个比较有代表性的基于密度的聚类算法。与划分和层次聚类方法不同，它将簇定义为密度相连的点的最大集合，能够把具有足够高密度的区域划分为簇，并可在噪声的空间数据库中发现任意形状的聚类。
基于DBSCAN聚类的异常检测算法将规模过小的簇视为异常。

### 使用方法

使用DBSCAN算法进行异常检测需要设置簇最小规模(minPoints)，聚类结果中规模小于或等于minPoints的簇中的样本预测为异常，在预测结果中，outlier值为true，label值为-1，
score值越大表示样本构成的簇越稀疏；对于规模大于minPoints的簇，label非负，表示簇的编号。如果没有设置聚类半径(Epsilon)，算法将自动选择合适的值。

### 距离度量方式

| 参数名称      | 参数描述                                                                          | 说明  |
|-----------|-------------------------------------------------------------------------------|-----|
| EUCLIDEAN | <img src="https://img.alicdn.com/tfs/TB1sSQoa.z1gK0jSZLeXXb9kVXa-211-39.png"> | 欧式距离 |
| COSINE    | <img src="https://img.alicdn.com/tfs/TB1P9Iqa7H0gK0jSZPiXXavapXa-263-61.png"> | 夹角余弦距离 |
| CITYBLOCK | d(x-c) = &#124; x-c &#124;                                                    | 街区距离 |

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN", "COSINE", "INNERPRODUCT", "CITYBLOCK", "JACCARD", "PEARSON" | "EUCLIDEAN" |
| epsilon | 样本邻域半径 | 样本邻域半径 | Double |  | x >= 0.0 |  |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| maxOutlierNumPerGroup | 每组最大异常点数目 | 每组最大异常点数目 | Integer |  |  |  |
| maxOutlierRatio | 最大异常点比例 | 算法检测异常点的最大比例 | Double |  |  |  |
| maxSampleNumPerGroup | 每组最大样本数目 | 每组最大样本数目 | Integer |  |  |  |
| minPoints | 邻域样本数 | 邻域样本数 | Integer |  | x >= 0 | 3 |
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
               [1,0],
               [1,0],
               [1,0],
               [1,0],
               [1,0],
               [6.0]
        ])
source = BatchOperator.fromDataframe(df, schemaStr='f0 double')

DbscanOutlierBatchOp()\
			.setFeatureCols(["f0"])\
			.setPredictionCol("outlier")\
			.setPredictionDetailCol("details")\
			.setMinPoints(4)\
			.linkFrom(source)\
			.print()
    
```

### JAVA 代码

```java
package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DbscanOutlierBatchOpTest {

	@Test
	public void test() throws Exception {
		List <Row> data = Arrays.asList(
			Row.of(1.0),
			Row.of(1.0),
			Row.of(1.0),
			Row.of(1.0),
			Row.of(1.0),
			Row.of(6.0)
		);
		String schemaStr = "f0 double";
		BatchOperator source = new MemSourceBatchOp(data, schemaStr);
		new DbscanOutlierBatchOp()
			.setFeatureCols(new String[] {"f0"})
			.setPredictionCol("outlier")
			.setPredictionDetailCol("details")
			.setMinPoints(4)
			.linkFrom(source)
			.print();
	}
}
```
### 运行结果

f0 |outlier|details
---|-------|-------
1.0000|false|{"outlier_score":"0.0","cluster_size":"5","label":"0","is_outlier":"false"}
1.0000|false|{"outlier_score":"0.0","cluster_size":"5","label":"0","is_outlier":"false"}
1.0000|false|{"outlier_score":"0.0","cluster_size":"5","label":"0","is_outlier":"false"}
1.0000|false|{"outlier_score":"0.0","cluster_size":"5","label":"0","is_outlier":"false"}
1.0000|false|{"outlier_score":"0.0","cluster_size":"5","label":"0","is_outlier":"false"}
6.0000|true|{"outlier_score":"1.8541019662496845","cluster_size":"1","label":"-1","is_outlier":"true"}
