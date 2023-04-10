# 标签传播分类 (CommunityDetectionClassifyBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.CommunityDetectionClassifyBatchOp

Python 类名：CommunityDetectionClassifyBatchOp


## 功能介绍

该算法为半监督的分类算法，原理为用已标记节点的标签信息去预测未标记节点的标签信息。 
在算法执行过程中，每个节点的标签按相似度传播给相邻节点，在节点传播的每一步，
每个节点根据相邻节点的标签来更新自己的标签，与该节点相似度越大，其相邻节点对其标注的影响权值越大，
相似节点的标签越趋于一致，其标签就越容易传播。在标签传播过程中，保持已标注数据的标签不变，
使其像一个源头把标签传向未标注数据。 最终，当迭代过程结束时，相似节点的概率分布也趋于相似，
可以划分到同一个类别中，从而完成标签传播过程。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| vertexCol | 输入点表中点所在列 | 输入点表中点所在列 | String | ✓ |  |  |
| vertexLabelCol | 输入点表中标签所在列 | 输入点表中标签所在列 | String | ✓ |  |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |
| delta | delta | delta参数 | Double |  |  | 0.2 |
| edgeWeightCol | 边权重列 | 表示边权重的列 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| k | K值 | 每轮迭代中，设置1/k的node不更新它们的值。这样的设定可能使得社区发现的效果更好。 | Integer |  |  | 40 |
| maxIter | 最大迭代次数 | 最大迭代次数 | Integer |  | x >= 1 | 50 |
| vertexWeightCol | 点的权重所在列 | 点的权重所在列，如果不输入就自动补为1。 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |



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

df2 = pd.DataFrame([[2, 0],\
[4, 1],\
[7, 1],\
[8, 1],\
[9, 2],\
[10, 2]])

verteices = BatchOperator.fromDataframe(df2, schemaStr="vertex int, label bigint")

communityDetectionClassify = CommunityDetectionClassifyBatchOp()\
                .setEdgeSourceCol("source")\
                .setEdgeTargetCol("target")\
                .setVertexCol("vertex")\
                .setVertexLabelCol("label")
communityDetectionClassify.linkFrom(edges, verteices).print()
```

### Java代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CommunityDetectionClassifyBatchOpTest {
	@Test
	public void test() throws Exception {
		List <Row> edgeRows = Arrays.asList(
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

		BatchOperator edges = new MemSourceBatchOp(edgeRows,  "source int, target int");

		List <Row> vertexRows = Arrays.asList(
			Row.of(2, 0L),
			Row.of(4, 1L),
			Row.of(7, 1L),
			Row.of(8, 1L),
			Row.of(9, 2L),
			Row.of(10, 2L));

		BatchOperator verteices = new MemSourceBatchOp(vertexRows,"vertex int, label bigint");

		new CommunityDetectionClassifyBatchOp()
                .setEdgeSourceCol("source")
                .setEdgeTargetCol("target")
                .setVertexCol("vertex")
                .setVertexLabelCol("label")
				.linkFrom(edges, verteices).print();
	}
}
```

### 运行结果

vertex|label
------|-----
0|0
10|2
2|0
12|2
6|1
9|2
1|0
13|2
11|2
3|0
4|1
5|1
8|1
7|1
