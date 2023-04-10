# DBSCAN异常检测 (DbscanOutlierStreamOp)
Java 类名：com.alibaba.alink.operator.stream.outlier.DbscanOutlierStreamOp

Python 类名：DbscanOutlierStreamOp


## 功能介绍

[DBSCAN](https://en.wikipedia.org/wiki/DBSCAN)，Density-Based Spatial Clustering of Applications with
Noise，是一个比较有代表性的基于密度的聚类算法。与划分和层次聚类方法不同，它将簇定义为密度相连的点的最大集合，能够把具有足够高密度的区域划分为簇，并可在噪声的空间数据库中发现任意形状的聚类。
基于DBSCAN聚类的异常检测算法将规模过小的簇视为异常。

### 使用方法

使用DBSCAN算法进行异常检测需要设置簇最小规模(minPoints)，聚类结果中规模小于或等于minPoints的簇中的样本预测为异常，在预测结果中，outlier值为true，label值为-1，
score值越大表示样本构成的簇越稀疏；对于规模大于minPoints的簇，label非负，表示簇的编号。如果没有设置聚类半径(Epsilon)，算法将自动选择合适的值。

与批式算法使用全部样本计算不同，流式算法使用从开始时刻到当前时刻的历史数据计算当前样本是否是一个异常，数据源需要有时间列。
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
| minPoints | 邻域样本数 | 邻域样本数 | Integer |  | x >= 0 | 3 |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| precedingRows | 数据窗口大小 | 数据窗口大小 | Integer |  |  | null |
| precedingTime | 时间窗口大小 | 时间窗口大小 | String |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| tensorCol | tensor列 | tensor列 | String |  | 所选列类型为 [BOOL_TENSOR, BYTE_TENSOR, DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR, STRING, STRING_TENSOR, TENSOR, UBYTE_TENSOR] | null |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String |  |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

import time, datetime
import numpy as np
import pandas as pd

data = pd.DataFrame([
			[0,1, 49.0, 2.0, datetime.datetime.fromtimestamp(1512057600000 / 1000)],
			[1,1, 50.0, 2.0, datetime.datetime.fromtimestamp(1512144000000 / 1000)],
			[2,1, 50.0, 1.0, datetime.datetime.fromtimestamp(1512230400000 / 1000)],
			[3,0, 2.0, 5.0, datetime.datetime.fromtimestamp(1512316800000 / 1000)],
			[4,0, 2.0, 5.0, datetime.datetime.fromtimestamp(1512403200000 / 1000)],
			[5,0, 3.0, 4.0, datetime.datetime.fromtimestamp(1512489600000 / 1000)],
			[6,0, 3.0, 4.0, datetime.datetime.fromtimestamp(1512576000000 / 1000)],
			[7,0, 3.0, 4.0, datetime.datetime.fromtimestamp(1512662400000 / 1000)],
			[8,0, 3.0, 4.0, datetime.datetime.fromtimestamp(1512748800000 / 1000)],
			[9,0, 3.0, 4.0, datetime.datetime.fromtimestamp(1512835200000 / 1000)]
])
dataOp = dataframeToOperator(data, schemaStr='id int, group_id int, f0 double, f1 double, ts timestamp', op_type='stream')
outlierOp = DbscanOutlierStreamOp()\
			.setGroupCols(["group_id"])\
			.setTimeCol("ts")\
			.setMinPoints(3)\
			.setFeatureCols(["f0"])\
			.setPredictionCol("pred")\
			.setPredictionDetailCol("pred_detail")

dataOp.link(outlierOp).print()

StreamOperator.execute()
```

### Java 代码
```java
package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.outlier.DbscanOutlierStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class DbscanOutlierStreamOpTest{
	public void testDbscanOutlierStreamOp() throws Exception {
		List <Row> rows = Arrays.asList(
			Row.of(0,1, 49.0, 2.0, new Timestamp(1512057600000L)),
			Row.of(1,1, 50.0, 2.0, new Timestamp(1512144000000L)),
			Row.of(2,1, 50.0, 1.0, new Timestamp(1512230400000L)),
			Row.of(3,0, 2.0, 5.0,  new Timestamp(1512316800000L)),
			Row.of(4,0, 2.0, 5.0,  new Timestamp(1512403200000L)),
			Row.of(5,0, 3.0, 4.0,  new Timestamp(1512489600000L)),
			Row.of(6,0, 3.0, 4.0,  new Timestamp(1512576000000L)),
			Row.of(7,0, 3.0, 4.0,  new Timestamp(1512662400000L)),
			Row.of(8,0, 3.0, 4.0,  new Timestamp(1512748800000L)),
			Row.of(9,0, 3.0, 4.0,  new Timestamp(1512835200000L)),
			Row.of(10,0, 3.0, 4.0,  new Timestamp(1512921600000L)),
			Row.of(11,0, 4.0, 3.0,  new Timestamp(1513008000000L)),
			Row.of(12,0, 4.0, 3.0,  new Timestamp(1513094400000L)),
			Row.of(13,0, 4.0, 3.0,  new Timestamp(1513180800000L)),
			Row.of(14,0, 4.0, 3.0,  new Timestamp(1513267200000L)),
			Row.of(15,0, 4.0, 3.0,  new Timestamp(1513353600000L)),
			Row.of(16,0, 4.0, 3.0,  new Timestamp(1513440000000L)),
			Row.of(17,0, 5.0, 2.0,  new Timestamp(1513526400000L)),
			Row.of(18,0, 5.0, 2.0,  new Timestamp(1513612800000L)),
			Row.of(19,0, 6.0, 1.0,  new Timestamp(1513699200000L)),
			Row.of(20,0, 50.0, 0.0, new Timestamp(1513785600000L)),
			Row.of(21,1, 50.0, 0.0, new Timestamp(1513872000000L))
		);
		String[] schema = new String[] {"id","group_id", "f0", "f1", "ts"};
		StreamOperator source = new MemSourceStreamOp(rows, schema);
		new DbscanOutlierStreamOp()
			.setFeatureCols(new String[] {"f0"})
			.setTimeCol("ts")
			.setGroupCols("group_id")
			.setPredictionCol("outlier")
			.setPredictionDetailCol("details")
			.setMinPoints(3)
			.linkFrom(source)
			.print();
		StreamOperator.execute();
	}

}

```
### 运行结果

id |group_id|f0 |f1 |ts |outlier|details
---|--------|---|---|---|-------|-------
0|1|49.0000|2.0000|2017-12-01 00:00:00.0|true|{"outlier_score":"NaN","cluster_size":"1","label":"-1","is_outlier":"true"}
3|0|2.0000|5.0000|2017-12-04 00:00:00.0|true|{"outlier_score":"NaN","cluster_size":"1","label":"-1","is_outlier":"true"}
4|0|2.0000|5.0000|2017-12-05 00:00:00.0|true|{"outlier_score":"NaN","cluster_size":"1","label":"-1","is_outlier":"true"}
1|1|50.0000|2.0000|2017-12-02 00:00:00.0|true|{"outlier_score":"NaN","cluster_size":"1","label":"-1","is_outlier":"true"}
2|1|50.0000|1.0000|2017-12-03 00:00:00.0|true|{"outlier_score":"NaN","cluster_size":"1","label":"-1","is_outlier":"true"}
5|0|3.0000|4.0000|2017-12-06 00:00:00.0|true|{"outlier_score":"NaN","cluster_size":"1","label":"-1","is_outlier":"true"}
21|1|50.0000|0.0000|2017-12-22 00:00:00.0|false|{"outlier_score":"1.0","cluster_size":"4","label":"0","is_outlier":"false"}
6|0|3.0000|4.0000|2017-12-07 00:00:00.0|false|{"outlier_score":"1.0","cluster_size":"4","label":"0","is_outlier":"false"}
7|0|3.0000|4.0000|2017-12-08 00:00:00.0|false|{"outlier_score":"1.0","cluster_size":"5","label":"0","is_outlier":"false"}
8|0|3.0000|4.0000|2017-12-09 00:00:00.0|false|{"outlier_score":"1.2426406871192852E-18","cluster_size":"4","label":"2","is_outlier":"false"}
9|0|3.0000|4.0000|2017-12-10 00:00:00.0|false|{"outlier_score":"1.3559906035297762E-18","cluster_size":"5","label":"2","is_outlier":"false"}
10|0|3.0000|4.0000|2017-12-11 00:00:00.0|false|{"outlier_score":"1.4641016151377545E-18","cluster_size":"6","label":"2","is_outlier":"false"}
11|0|4.0000|3.0000|2017-12-12 00:00:00.0|true|{"outlier_score":"1.242640687119285","cluster_size":"1","label":"-1","is_outlier":"true"}
12|0|4.0000|3.0000|2017-12-13 00:00:00.0|true|{"outlier_score":"1.1237243569579451","cluster_size":"2","label":"-1","is_outlier":"true"}
13|0|4.0000|3.0000|2017-12-14 00:00:00.0|true|{"outlier_score":"1.0498962651136545","cluster_size":"3","label":"-1","is_outlier":"true"}
14|0|4.0000|3.0000|2017-12-15 00:00:00.0|false|{"outlier_score":"1.8541019662496845E-18","cluster_size":"4","label":"8","is_outlier":"false"}
15|0|4.0000|3.0000|2017-12-16 00:00:00.0|false|{"outlier_score":"1.9430780487613657E-18","cluster_size":"5","label":"8","is_outlier":"false"}
16|0|4.0000|3.0000|2017-12-17 00:00:00.0|false|{"outlier_score":"2.029285639896449E-18","cluster_size":"6","label":"8","is_outlier":"false"}
17|0|5.0000|2.0000|2017-12-18 00:00:00.0|true|{"outlier_score":"1.6666666666666665","cluster_size":"1","label":"-1","is_outlier":"true"}
18|0|5.0000|2.0000|2017-12-19 00:00:00.0|true|{"outlier_score":"1.4641016151377544","cluster_size":"2","label":"-1","is_outlier":"true"}
19|0|6.0000|1.0000|2017-12-20 00:00:00.0|true|{"outlier_score":"2.6675105012029814","cluster_size":"1","label":"-1","is_outlier":"true"}
20|0|50.0000|0.0000|2017-12-21 00:00:00.0|true|{"outlier_score":"3.441362272888248","cluster_size":"1","label":"-1","is_outlier":"true"}

备注：id小于6的数据，因为流式计算时每组的样本还未累计超过三个，所以记为异常，outlier_score为NaN。
