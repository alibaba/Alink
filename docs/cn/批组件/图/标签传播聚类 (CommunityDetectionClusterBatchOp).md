# 标签传播聚类 (CommunityDetectionClusterBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.CommunityDetectionClusterBatchOp

Python 类名：CommunityDetectionClusterBatchOp


## 功能介绍

图聚类是根据图的拓扑结构，进行子图的划分，使得子图内部节点的链接较多，子图之间的连接较少。
标签传播聚类算法的基本思路是节点的标签依赖其邻居节点的标签信息，影响程度由节点相似度决定，并通过传播迭代更新达到稳定。

标签传播聚类是一个迭代的过程，每一步迭代中，标签沿着边将自己的标签传给相邻的节点，相邻节点接收到所有边传输过来的标签后，计算节点所属的标签。
在下一轮传播时，继续将更新后的标签传播给自己的相邻节点。直到所有节点的标签不再改变。

## 参数说明



| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| vertexCol | 输入点表中点所在列 | 输入点表中点所在列 | String | ✓ |  |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |
| delta | delta | delta参数 | Double |  | x > 0.0 | 0.2 |
| edgeWeightCol | 边权重列 | 表示边权重的列 | String |  |  | null |
| k | K值 | 每轮迭代中，设置1/k的node不更新它们的值。这样的设定可能使得社区发现的效果更好。 | Integer |  | x >= 1 | 40 |
| maxIter | 最大迭代次数 | 最大迭代次数 | Integer |  | x >= 1 | 50 |
| vertexWeightCol | 点的权重所在列 | 点的权重所在列，如果不输入就自动补为1。 | String |  |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([[3, 1],\
[3, 0],\
[0, 1],\
[0, 2],\
[2, 1],\
[2, 4],\
[5, 4],\
[7, 4],\
[5, 6],\
[5, 8],\
[5, 7],\
[7, 8],\
[6, 8],\
[12, 10],\
[12, 11],\
[12, 13],\
[12, 9],\
[10, 9],\
[8, 9],\
[13, 9],\
[10, 7],\
[10, 11],\
[11, 13]])

edges = BatchOperator.fromDataframe(df, schemaStr="source int, target int")

communityDetectionClusterBatchOp = CommunityDetectionClusterBatchOp()\
    .setEdgeSourceCol("source")\
    .setEdgeTargetCol("target")
communityDetectionClusterBatchOp.linkFrom(edges).print()
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

public class CommunityDetectionClusterBatchOpTest {
	@Test
	public void testIntWithVertex() throws Exception {
		List<Row> datas = Arrays.asList(
				Row.of(3, 1),
				Row.of(3, 0),
				Row.of(0, 1),
				Row.of(0, 2),
				Row.of(2, 1),
				Row.of(2, 4),
				Row.of(5, 4),
				Row.of(7, 4),
				Row.of(5, 6),
				Row.of(5, 8),
				Row.of(5, 7),
				Row.of(7, 8),
				Row.of(6, 8),
				Row.of(12, 10),
				Row.of(12, 11),
				Row.of(12, 13),
				Row.of(12, 9),
				Row.of(10, 9),
				Row.of(8, 9),
				Row.of(13, 9),
				Row.of(10, 7),
				Row.of(10, 11),
				Row.of(11, 13));

		BatchOperator edges = new MemSourceBatchOp(datas, "source int, target int");

		new CommunityDetectionClusterBatchOp()
    		.setEdgeSourceCol("source")
    		.setEdgeTargetCol("target")
			.linkFrom(edges).print();
	}
}
```

### 运行结果


vertex|label
------|-----
0|0
10|1
2|0
12|1
6|0
9|1
1|0
13|1
11|1
3|0
4|0
5|0
8|0
7|0


