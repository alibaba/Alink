# DBSCAN (DbscanBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.DbscanBatchOp

Python 类名：DbscanBatchOp


## 功能介绍

[DBSCAN](https://en.wikipedia.org/wiki/DBSCAN)，Density-Based Spatial Clustering of Applications with Noise，是一个比较有代表性的基于密度的聚类算法。与划分和层次聚类方法不同，它将簇定义为密度相连的点的最大集合，能够把具有足够高密度的区域划分为簇，并可在噪声的空间数据库中发现任意形状的聚类。
<div align=center><img src="https://img.alicdn.com/tfs/TB19W7uaW61gK0jSZFlXXXDKFXa-700-431.png" height="50%" width="50%"></div>

本算法为DBSCAN对应的训练组件，输入为训练数据，输出有两个：（1）每个数据点的ID，节点类型以及聚类中心，（2）可以用来预测新数据的DBSCAN模型。

### 距离度量方式
| 参数名称 | 参数描述 | 说明 |
| --- | --- | --- |
| EUCLIDEAN | <img src="https://img.alicdn.com/imgextra/i3/O1CN01fpjOXj1rCYlGWvP7w_!!6000000005595-2-tps-1624-290.png"> | 欧式距离 |
| COSINE | <img src="https://img.alicdn.com/imgextra/i2/O1CN01w80YFW1tl1nZ5qEBN_!!6000000005941-2-tps-1676-330.png"> | 夹角余弦距离 |
| CITYBLOCK | <img src="https://img.alicdn.com/imgextra/i4/O1CN01aTf1Zt1aVw2vjuYw1_!!6000000003336-2-tps-1666-280.png"> | 街区距离 |

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| epsilon | 邻域距离阈值 | 邻域距离阈值 | Double | ✓ |  |  |
| idCol | ID列名 | ID列对应的列名 | String | ✓ |  |  |
| minPoints | 邻域中样本个数的阈值 | 邻域中样本个数的阈值 | Integer | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |
| distanceType | 距离度量方式 | 距离类型 | String |  | "EUCLIDEAN", "COSINE", "CITYBLOCK" | "EUCLIDEAN" |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

data = pd.DataFrame([
        ["id_1", "2.0,3.0"],
        ["id_2", "2.1,3.1"],
        ["id_3", "200.1,300.1"],
        ["id_4", "200.2,300.2"],
        ["id_5", "200.3,300.3"],
        ["id_6", "200.4,300.4"],
        ["id_7", "200.5,300.5"],
        ["id_8", "200.6,300.6"],
        ["id_9", "2.1,3.1"],
        ["id_10", "2.1,3.1"],
        ["id_11", "2.1,3.1"],
        ["id_12", "2.1,3.1"],
        ["id_16", "300.,3.2"]
])

inOp1 = BatchOperator.fromDataframe(data, schemaStr='id string, vec string')
inOp2 = StreamOperator.fromDataframe(data, schemaStr='id string, vec string')

dbscan = DbscanBatchOp()\
    .setIdCol("id")\
    .setVectorCol("vec")\
    .setMinPoints(3)\
    .setEpsilon(0.5)\
    .setPredictionCol("pred")\
    .linkFrom(inOp1)

dbscan.print()

predict = DbscanPredictBatchOp()\
    .setPredictionCol("pred")\
    .linkFrom(dbscan.getSideOutput(0), inOp1)
    
predict.print()

predict = DbscanPredictStreamOp(dbscan.getSideOutput(0))\
    .setPredictionCol("pred")\
    .linkFrom(inOp2)
    
predict.print()

StreamOperator.execute()
```

#### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.DbscanPredictStreamOp;
import com.alibaba.alink.operator.batch.clustering.DbscanPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.DbscanBatchOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DbscanBatchOpTest {

	@Test
	public void testDbscanBatchop() throws Exception {
		List <Row> dataPoints = Arrays.asList(
			Row.of("id_1", "2.0,3.0"),
			Row.of("id_2", "2.1,3.1"),
			Row.of("id_3", "200.1,300.1"),
			Row.of("id_4", "200.2,300.2"),
			Row.of("id_5", "200.3,300.3"),
			Row.of("id_6", "200.4,300.4"),
			Row.of("id_7", "200.5,300.5"),
			Row.of("id_8", "200.6,300.6"),
			Row.of("id_9", "2.1,3.1"),
			Row.of("id_10", "2.1,3.1"),
			Row.of("id_11", "2.1,3.1"),
			Row.of("id_12", "2.1,3.1"),
			Row.of("id_16", "300.,3.2"));

		MemSourceBatchOp inOp1 = new MemSourceBatchOp(dataPoints, "id string, vec string");
		MemSourceStreamOp inOp2 = new MemSourceStreamOp(dataPoints, "id string, vec string");

		DbscanBatchOp dbscanBatchOp = new DbscanBatchOp()
			.setIdCol("id")
			.setVectorCol("vec")
			.setMinPoints(3)
			.setEpsilon(0.5)
			.setPredictionCol("pred")
			.linkFrom(inOp1);
		dbscanBatchOp.print();

		DbscanPredictBatchOp dbscanPredictBatchOp = new DbscanPredictBatchOp()
			.setPredictionCol("pred")
			.linkFrom(dbscanBatchOp.getSideOutput(0), inOp1);
		dbscanPredictBatchOp.print();

		DbscanPredictStreamOp dbscanPredictStreamOp = new DbscanPredictStreamOp(dbscanBatchOp.getSideOutput(0))
			.setPredictionCol("pred")
			.linkFrom(inOp2);
		dbscanPredictStreamOp.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
#### 训练结果
id|type|pred
---|----|----
id_4|CORE|1
id_8|CORE|1
id_2|CORE|0
id_6|CORE|1
id_16|NOISE|-2147483648
id_7|CORE|1
id_12|CORE|0
id_5|CORE|1
id_1|CORE|0
id_3|CORE|1
id_9|CORE|0
id_10|CORE|0
id_11|CORE|0

#### 批式预测结果
id|vec|pred
---|---|----
id_1|2.0,3.0|0
id_2|2.1,3.1|0
id_3|200.1,300.1|1
id_4|200.2,300.2|1
id_5|200.3,300.3|1
id_6|200.4,300.4|1
id_7|200.5,300.5|1
id_8|200.6,300.6|1
id_9|2.1,3.1|0
id_10|2.1,3.1|0
id_11|2.1,3.1|0
id_12|2.1,3.1|0
id_16|300.,3.2|-2147483648

#### 流式预测结果
id|vec|pred
---|---|----
id_11|2.1,3.1|0
id_1|2.0,3.0|0
id_16|300.,3.2|-2147483648
id_12|2.1,3.1|0
id_6|200.4,300.4|1
id_3|200.1,300.1|1
id_7|200.5,300.5|1
id_9|2.1,3.1|0
id_2|2.1,3.1|0
id_10|2.1,3.1|0
id_4|200.2,300.2|1
id_5|200.3,300.3|1
id_8|200.6,300.6|1

## 备注
### 资源如何预估
- 每个worker的内存大小如何估计？
  - 将输入数据的大小乘以15，即假设输入数据的大小是1GB，那么每个worker的大小可以设置为15GB。
- 如何设置worker的个数？
  - 一般情况下，随着worker数目的增加，由于通信开销的存在，分布式训练任务会先变快，然后变慢。用户如果观测到worker数目增加之后，速度变慢，那么应该停止增加worker数目。
- 本算法可以支持多大的数据量？
  - 数据量小于100万条，维度小于200。如果数据超过了这个范围，建议先将数据分组，然后每个组分别跑DBSCAN算法。
- 为什么一个中心点的聚类中心ID是-2147483648?
  - 因为该数据点是离散点，它不属于任何一个聚类中心。

### 参数如何设置
DBSCAN中常用的两个参数为：临域中样本个数的阈值(minPoints) 和 临域距离阈值(epsilon): 
- 当观测到cluster数目过多，想要减少cluster数目时，建议调大minPoints，调小epsilon（建议优先调节minPoints）。
- 当观测到cluster数目过少，想要增加cluster数目时，建议调小minPoints，调大epsilon（建议优先调节minPoints）。
