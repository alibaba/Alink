# 分组Kmeans (GroupKMeansBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.GroupKMeansBatchOp

Python 类名：GroupKMeansBatchOp


## 功能介绍
分组Kmeans聚类算法。本算法按照用户指定的分组列（groupCol）将数据分成很多个组，然后分别对这些组进行Kmeans聚类。算法的输出值是每个数据点的所属类别。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| groupCols | 分组列名，多列 | 分组列名，多列，必选 | String[] | ✓ |  |  |
| idCol | id列名 | id列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| distanceType | 距离度量方式 | 距离类型 | String |  | "EUCLIDEAN", "COSINE", "CITYBLOCK" | "EUCLIDEAN" |
| epsilon | 收敛阈值 | 当两轮迭代的中心点距离小于epsilon时，算法收敛。 | Double |  |  | 1.0E-4 |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  |  | 2 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 10。 | Integer |  |  | 10 |


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

groupKmeans = GroupKMeansBatchOp()\
    .setGroupCols(["group"])\
    .setK(2)\
    .setMaxIter(50)\
    .setPredictionCol("pred")\
    .setEpsilon(1e-8)\
    .setFeatureCols(["c1", "c2"])\
    .setIdCol("id")\
    .linkFrom(source)

groupKmeans.print()
```
### Java 代码
```java
package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.clustering.GroupKMeansBatchOp;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GroupKmeansBatchOpTest {

	@Test
	public void testGroupKmeansBatchOp() throws Exception {
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
		GroupKMeansBatchOp op = new GroupKMeansBatchOp()
			.setGroupCols(new String[] {"group"})
			.setK(2)
			.setMaxIter(50)
			.setPredictionCol("pred")
			.setEpsilon(1e-8)
			.setFeatureCols(new String[] {"c1", "c2"})
			.setIdCol("id")
			.linkFrom(inputOp);
		op.print();
	}
}
```

### 运行结果

#### 预测结果
```
group|id|pred
-----|---|----
1|id_10|0
1|id_17|0
1|id_13|0
1|id_6|1
1|id_9|0
1|id_11|0
1|id_14|0
1|id_20|0
1|id_7|1
1|id_16|1
 ......
1|id_21|0
0|id_2|0
0|id_3|1
0|id_15|0
0|id_1|0
0|id_18|0
0|id_12|0
0|id_8|1
0|id_4|1
2|id_20|0
```

