# 最大联通分量 (ConnectedComponentsBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.ConnectedComponentsBatchOp

Python 类名：ConnectedComponentsBatchOp


## 功能介绍

在无向图G中，若从顶点A到顶点B有路径相连，则称A和B是连通的；
在图G种存在若干子图，其中每个子图中所有顶点之间都是连通的，但在不同子图间不存在顶点连通，那么称图G的这些子图为最大连通子图。

最大联通子图的应用场景有ID-Mapping，节点分类，社区发现，反作弊等。比如识别作弊或者有风险的账号，与作弊账号在同一个子图中的账号，就有潜在的风险。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |
| maxIter | 最大迭代次数 | 最大迭代次数 | Integer |  | x >= 1 | 50 |
| vertexCol | 边的源顶点 | 边的源顶点 | String |  |  | "vertex" |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([[1, 2],\
[2, 3],\
[3, 4],\
[4, 5],\
[6, 7],\
[7, 8],\
[8, 9],\
[9, 6]])

data = BatchOperator.fromDataframe(df, schemaStr="source int, target int")
ConnectedComponentsBatchOp()\
    .setEdgeSourceCol("source")\
    .setEdgeTargetCol("target")\
    .linkFrom(data)\
    .print()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ConnectedComponentsBatchOpTest {
	@Test
	public void test() throws Exception {
		List<Row> edgeRows = Arrays.asList(
			Row.of(1, 2),
			Row.of(2, 3),
			Row.of(3, 4),
			Row.of(4, 5),
			Row.of(6, 7),
			Row.of(7, 8),
			Row.of(8, 9),
			Row.of(9, 6)
		);
		BatchOperator edgeData = new MemSourceBatchOp(edgeRows, "source int,target int");

		BatchOperator res = new ConnectedComponentsBatchOp()
			.setSetStable(false)
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(edgeData).print();
	}
}
```

### 运行结果

node|groupId
----|-------
1|0
2|0
5|0
9|4
4|0
3|0
8|4
6|4
7|4


