# 分组Dbscan (GroupDbscanBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.GroupDbscanBatchOp

Python 类名：GroupDbscanBatchOp


## 功能介绍
[DBSCAN](https://en.wikipedia.org/wiki/DBSCAN)，Density-Based Spatial Clustering of Applications with Noise，是一个比较有代表性的基于密度的聚类算法。与划分和层次聚类方法不同，它将簇定义为密度相连的点的最大集合，能够把具有足够高密度的区域划分为簇，并可在噪声的空间数据库中发现任意形状的聚类。

分组DBSCAN算法根据用户指定的"分组列"将输入数据分为多个组，然后在每个组内部进行DBSCAN聚类算法。

![](https://cdn.nlark.com/lark/0/2018/png/18345/1542600186110-148173c0-c424-4235-af57-415f2168f07a.png#alt=1042406-20161222112847323-1346197243.png)

##### 距离度量方式
| 参数名称 | 参数描述 | 说明 |
| --- | --- | --- |
| EUCLIDEAN | ![](https://zos.alipayobjects.com/rmsportal/yVXAleRfvuwhJWJ.png#alt=image) | 欧式距离 |
| COSINE | ![](https://zos.alipayobjects.com/rmsportal/nmYGZXRGLqpQhbX.png#alt=image) | 夹角余弦距离 |
| CITYBLOCK | ![](https://zos.alipayobjects.com/rmsportal/jkApzArpNWDJFbV.png#alt=image) | 城市街区距离，也称曼哈顿距离 |

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| epsilon | 邻域距离阈值 | 邻域距离阈值 | Double | ✓ |  |  |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| groupCols | 分组列名，多列 | 分组列名，多列，必选 | String[] | ✓ |  |  |
| idCol | id列名 | id列名 | String | ✓ |  |  |
| minPoints | 邻域中样本个数的阈值 | 邻域中样本个数的阈值 | Integer | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN", "COSINE", "CITYBLOCK", "HAVERSINE", "JACCARD" | "EUCLIDEAN" |
| groupMaxSamples | 每个分组的最大样本数 | 每个分组的最大样本数 | Integer |  |  | 2147483647 |
| isOutputVector | 输出是否为向量格式 | 输出是否为向量格式 | Boolean |  |  | false |
| skip | 每个分组超过最大样本数时，是否跳过 | 每个分组超过最大样本数时，是否跳过 | Boolean |  |  | false |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, "id_1", 2.0, 3.0],
    [0, "id_2", 2.1, 3.1],
    [0, "id_18", 2.4, 3.2],
    [0, "id_15", 2.8, 3.2],
    [0, "id_12", 2.1, 3.1],
    [0, "id_3", 200.1, 300.1],
    [0, "id_4", 200.2, 300.2],
    [0, "id_8", 200.6, 300.6],

    [1, "id_5", 200.3, 300.3],
    [1, "id_6", 200.4, 300.4],
    [1, "id_7", 200.5, 300.5],
    [1, "id_16", 300., 300.2],
    [1, "id_9", 2.1, 3.1],
    [1, "id_10", 2.2, 3.2],
    [1, "id_11", 2.3, 3.3],
    [1, "id_13", 2.4, 3.4],
    [1, "id_14", 2.5, 3.5],
    [1, "id_17", 2.6, 3.6],
    [1, "id_19", 2.7, 3.7],
    [1, "id_20", 2.8, 3.8],
    [1, "id_21", 2.9, 3.9],

    [2, "id_20", 2.8, 3.8]])

source = BatchOperator.fromDataframe(df, schemaStr='group string, id string, c1 double, c2 double')

groupDbscan = GroupDbscanBatchOp()\
    .setIdCol("id")\
    .setGroupCols(["group"])\
    .setFeatureCols(["c1", "c2"])\
    .setMinPoints(4)\
    .setEpsilon(0.6)\
    .linkFrom(source)

groupDbscan.print()
```

#### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.clustering.GroupDbscanBatchOp;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GroupDbscanBatchOpTest {

	@Test
	public void testGroupDbscanBatchOp() throws Exception {
		List<Row> trainData = Arrays.asList(
			Row.of(0, "id_1", 2.0, 3.0),
			Row.of(0, "id_2", 2.1, 3.1),
			Row.of(0, "id_18", 2.4, 3.2),
			Row.of(0, "id_15", 2.8, 3.2),
			Row.of(0, "id_12", 2.1, 3.1),
			Row.of(0, "id_3", 200.1, 300.1),
			Row.of(0, "id_4", 200.2, 300.2),
			Row.of(0, "id_8", 200.6, 300.6),

			Row.of(1, "id_5", 200.3, 300.3),
			Row.of(1, "id_6", 200.4, 300.4),
			Row.of(1, "id_7", 200.5, 300.5),
			Row.of(1, "id_16", 300., 300.2),
			Row.of(1, "id_9", 2.1, 3.1),
			Row.of(1, "id_10", 2.2, 3.2),
			Row.of(1, "id_11", 2.3, 3.3),
			Row.of(1, "id_13", 2.4, 3.4),
			Row.of(1, "id_14", 2.5, 3.5),
			Row.of(1, "id_17", 2.6, 3.6),
			Row.of(1, "id_19", 2.7, 3.7),
			Row.of(1, "id_20", 2.8, 3.8),
			Row.of(1, "id_21", 2.9, 3.9),

			Row.of(2, "id_20", 2.8, 3.8)
		);

		MemSourceBatchOp inputOp = new MemSourceBatchOp(trainData,
			new String[] {"group", "id", "c1", "c2"});
		GroupDbscanBatchOp op = new GroupDbscanBatchOp()
			.setIdCol("id")
			.setGroupCols("group")
			.setFeatureCols("c1", "c2")
			.setMinPoints(4)
			.setEpsilon(0.6)
			.linkFrom(inputOp);
		op.print();
	}
}
```

### 运行结果
```
group|id|type|cluster_id|c1|c2
-----|---|----|----------|---|---
1|id_5|NOISE|-2147483648|200.3000|300.3000
1|id_6|NOISE|-2147483648|200.4000|300.4000
1|id_7|NOISE|-2147483648|200.5000|300.5000
1|id_16|NOISE|-2147483648|300.0000|300.2000
1|id_9|CORE|0|2.1000|3.1000
1|id_10|CORE|0|2.2000|3.2000
1|id_11|CORE|0|2.3000|3.3000
1|id_13|CORE|0|2.4000|3.4000
1|id_14|CORE|0|2.5000|3.5000
1|id_17|CORE|0|2.6000|3.6000
 ......
1|id_21|CORE|0|2.9000|3.9000
0|id_1|CORE|0|2.0000|3.0000
0|id_2|CORE|0|2.1000|3.1000
0|id_18|CORE|0|2.4000|3.2000
0|id_15|LINKED|0|2.8000|3.2000
0|id_12|CORE|0|2.1000|3.1000
0|id_3|NOISE|-2147483648|200.1000|300.1000
0|id_4|NOISE|-2147483648|200.2000|300.2000
0|id_8|NOISE|-2147483648|200.6000|300.6000
2|id_20|NOISE|-2147483648|2.8000|3.8000
```

