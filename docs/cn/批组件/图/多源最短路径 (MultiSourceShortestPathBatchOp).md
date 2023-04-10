# 多源最短路径 (MultiSourceShortestPathBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.MultiSourceShortestPathBatchOp

Python 类名：MultiSourceShortestPathBatchOp


## 功能介绍

给定的边以及多个源点，求图中所有点到给定源点的最短路径。输出所有点的根节点（给定源点之一）、距离和最短路径的节点序列。

对于不能连接到所有源点的节点，输出的根节点为null，距离为-1，节点序列为空。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| sourcePointCol | 源点的列 | 源点的列 | String | ✓ |  |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |
| edgeWeightCol | 边权重列 | 表示边权重的列 | String |  |  | null |
| maxIter | 最大迭代次数 | 最大迭代次数 | Integer |  | x >= 1 | 50 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([[0, 1, 0.4],
                   [1, 2, 1.3],
                   [2, 3, 1.0],
                   [3, 4, 1.0],
                   [4, 5, 1.0],
                   [5, 6, 1.0],
                   [6, 7, 1.0],
                   [7, 8, 1.0],
                   [8, 9, 1.0],
                   [9, 6, 1.0],
                   [19, 16, 1.0]])
source_df = pd.DataFrame([[1, 1],
                          [5, 5]])
data = BatchOperator.fromDataframe(df, schemaStr="source int, target int, weight double")
source_data = BatchOperator.fromDataframe(source_df, schemaStr="source int, target int")

MultiSourceShortestPathBatchOp()\
    .setEdgeSourceCol("source")\
    .setEdgeTargetCol("target")\
    .setEdgeWeightCol("weight")\
    .setSourcePointCol("source")\
    .linkFrom(data, source_data)\
    .print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MultiSourceShortestPathBatchOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		Row[] rows = new Row[]{
			Row.of(0, 1, 0.4),
			Row.of(1, 2, 1.3),
			Row.of(2, 3, 1.0),
			Row.of(3, 4, 1.0),
			Row.of(4, 5, 1.0),
			Row.of(5, 6, 1.0),
			Row.of(6, 7, 1.0),
			Row.of(7, 8, 1.0),
			Row.of(8, 9, 1.0),
			Row.of(9, 6, 1.0),
			Row.of(19, 16, 1.0),
		};
		Row[] sources = new Row[]{
			Row.of(1, 1),
			Row.of(5, 5),
		};
		BatchOperator inData = new MemSourceBatchOp(rows, new String[]{"source", "target", "weight"});
		BatchOperator sourceBatchOp = new MemSourceBatchOp(sources, new String[]{"source", "target"});
		MultiSourceShortestPathBatchOp op = new MultiSourceShortestPathBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setEdgeWeightCol("weight")
			.setSourcePointCol("source");

		BatchOperator res = op.linkFrom(inData, sourceBatchOp);

		res.lazyPrint(20);
	}

}
```

### 运行结果

vertex|root_node|node_list|distance
------|---------|---------|--------
19|null| |-1.0000
16|null| |-1.0000
0|1|0,1|0.4000
1|1|1|0.0000
2|1|2,1|1.3000
3|5|3,4,5|2.0000
4|5|4,5|1.0000
5|5|5|0.0000
8|5|8,7,6,5|3.0000
7|5|7,6,5|2.0000
6|5|6,5|1.0000
9|5|9,6,5|2.0000

##备注
源点个数少，且图中点非常多的情况下，运行速度会比较慢，这种情况建议使用单源最短路径算法。
