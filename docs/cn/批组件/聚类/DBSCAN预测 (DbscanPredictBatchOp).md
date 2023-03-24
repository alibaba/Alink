# DBSCAN预测 (DbscanPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.DbscanPredictBatchOp

Python 类名：DbscanPredictBatchOp


## 功能介绍
[DBSCAN](https://en.wikipedia.org/wiki/DBSCAN)，Density-Based Spatial Clustering of Applications with Noise，是一个比较有代表性的基于密度的聚类算法。与划分和层次聚类方法不同，它将簇定义为密度相连的点的最大集合，能够把具有足够高密度的区域划分为簇，并可在噪声的空间数据库中发现任意形状的聚类。
<div align=center><img src="https://img.alicdn.com/tfs/TB19W7uaW61gK0jSZFlXXXDKFXa-700-431.png" height="50%" width="50%"></div>

本算法为DBSCAN对应的预测组件，输入有两个，分别是：（1）DBSCAN模型（可以通过DBscan训练组件得到）（2）输出有一个，为预测数据。

### 距离度量方式
| 参数名称 | 参数描述 | 说明 |
| --- | --- | --- |
| EUCLIDEAN | <img src="https://img.alicdn.com/imgextra/i3/O1CN01fpjOXj1rCYlGWvP7w_!!6000000005595-2-tps-1624-290.png"> | 欧式距离 |
| COSINE | <img src="https://img.alicdn.com/imgextra/i2/O1CN01w80YFW1tl1nZ5qEBN_!!6000000005941-2-tps-1676-330.png"> | 夹角余弦距离 |
| CITYBLOCK | <img src="https://img.alicdn.com/imgextra/i4/O1CN01aTf1Zt1aVw2vjuYw1_!!6000000003336-2-tps-1666-280.png"> | 街区距离 |

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
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

inOp1 = BatchOperator.fromDataframe(df, schemaStr='id string, vec string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id string, vec string')

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

public class DbscanPredictBatchOpTest {

	@Test
	public void testDbscanPredictBatchop() throws Exception {
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
    - 将模型的大小乘以30，即假设输入数据的大小是1GB，那么每个worker的大小可以设置为30GB。
- 如何设置worker的个数？
    - 一般情况下，随着worker数目的增加，由于通信开销的存在，分布式训练任务会先变快，然后变慢。用户如果观测到worker数目增加之后，速度变慢，那么应该停止增加worker数目。
