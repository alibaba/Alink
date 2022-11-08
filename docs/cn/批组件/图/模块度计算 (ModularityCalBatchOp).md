# 模块度计算 (ModularityCalBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.ModularityCalBatchOp

Python 类名：ModularityCalBatchOp


## 功能介绍

模块度是一种评价图社群划分好坏的指标，来评估网络结构中划分出来社区的紧密程度，取值范围为-0.5到1之间。通常认为大于0.3的图是划分出较为明显社群的。

要计算一个网络的模块度，需要构造一个具有相同节点度分布的随机网络作为参照。通俗地来说，模块度的物理含义是：在社团内，实际的边数与随机情况下的边数的差距。如果差距比较大，说明社团内部密集程度显著高于随机情况，社团划分的质量较好。模块度取值范围在[-0.5,1]之间。如果节点组中的连边数量超过了随机分配时所得到的期望连边数量，模块度为正数。没有超过，则为负数。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| vertexCol | 点列 | 输入点表中点信息所在列 | String | ✓ |  |  |
| vertexCommunityCol | 社群信息列 | 输入点表中点的社群信息所在列 | String | ✓ | 所选列类型为 [INTEGER, LONG] |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |
| edgeWeightCol | 边权重列 | 表示边权重的列 | String |  |  | null |


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

vertices = BatchOperator.fromDataframe(df2, schemaStr="vertex int, label bigint")

communityDetectionClassify = CommunityDetectionClassifyBatchOp()\
    .setEdgeSourceCol("source")\
    .setEdgeTargetCol("target")\
    .setVertexCol("vertex")\
    .setVertexLabelCol("label")
community = communityDetectionClassify.linkFrom(edges, vertices)


modularityCal = ModularityCalBatchOp()\
    .setEdgeSourceCol("source")\
    .setEdgeTargetCol("target")\
    .setVertexCol("vertex")\
    .setVertexCommunityCol("label")
modularityCal.linkFrom(edges, community).print()
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

public class ModularityCalBatchOpTest extends AlinkTestBase {
	@Test
	public void testModularityCal() throws Exception {

		List <Row> edgesList = Arrays.asList(
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
		BatchOperator edges = new MemSourceBatchOp(edgesList, "source int, target int");
		List <Row> nodesList = Arrays.asList(Row.of(2, 0),
			Row.of(4, 1),
			Row.of(7, 1),
			Row.of(8, 1),
			Row.of(9, 2),
			Row.of(10, 2));
		BatchOperator nodes = new MemSourceBatchOp(nodesList, "vertex int, label int");

		CommunityDetectionClassifyBatchOp communityDetectionClassify = new CommunityDetectionClassifyBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setVertexCol("vertex")
			.setVertexLabelCol("label")
			.linkFrom(edges, nodes);

		ModularityCalBatchOp modularityCal = new ModularityCalBatchOp()
			.setSourceCol("source")
			.setTargetCol("target")
			.setVertexCol("vertex")
			.setVertexCommunityCol("label");
		modularityCal.linkFrom(edges, communityDetectionClassify).print();
	}
}
```

### 运行结果

| modularity       |
| --- |
| 0.522684        |

