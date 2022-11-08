# KCore算法 (KCoreBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.KCoreBatchOp

Python 类名：KCoreBatchOp


## 功能介绍

对于给定的图，反复去除图中度小于等于k的点，直到图中所有点的度均大于k或者达到最大迭代次数为止。

k-Core算法是一种用来在图中找出符合指定核心度的紧密关联的子图结构，在k-Core的结果子图中，每个顶点至少具有k的度数，且所有顶点都至少与该子图中的 k 个其他节点相连。k-Core通常用来对一个图进行子图划分，通过去除不重要的顶点，将符合逾期的子图暴露出来进行进一步分析。k-Core由于其线性的时间复杂度和符合直观认识的可解释性，在风控金融，社交网络和生物学上都具有较多的应用场景。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |
| k | k的数目 | 反复去除图中度小于等于k的点 | Integer |  | [1, +inf) | 3 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([[1, 2],\
        [1, 3],\
        [1, 4],\
        [2, 3],\
        [2, 4],\
        [3, 4],\
        [3, 5],\
        [3, 6],\
        [5, 6]])

data = BatchOperator.fromDataframe(df, schemaStr="source int, target int")
KCoreBatchOp()\
    .setEdgeSourceCol("source")\
    .setEdgeTargetCol("target")\
    .setK(2)\
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

public class KCoreBatchOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> intInputRows = Arrays.asList(
			Row.of(1, 2),
			Row.of(1, 3),
			Row.of(1, 4),
			Row.of(2, 3),
			Row.of(2, 4),
			Row.of(3, 4),
			Row.of(3, 5),
			Row.of(3, 6),
			Row.of(5, 6)
		);
		BatchOperator data = new MemSourceBatchOp(intInputRows, "source int, target int");
		KCoreBatchOp kCoreBatchOp = new KCoreBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setK(2)
			.linkFrom(data);
		kCoreBatchOp.print();
	}
}
```

### 运行结果


node1|node2
-----|-----
1|2
1|3
1|4
3|1
3|2
3|4
2|1
2|3
2|4
4|1
4|2
4|3
