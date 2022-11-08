# 树深度 (TreeDepthBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.TreeDepthBatchOp

Python 类名：TreeDepthBatchOp


## 功能介绍

对于给定的有向图，判断该有向图是否为一棵树或是否为森林。如果判定成功，则输出每个结点所在树的根结点以及该结点的深度。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |
| edgeWeightCol | 边权重列 | 表示边权重的列 | String |  |  | null |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | [1, +inf) | 100 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([[0, 1],\
[0, 2],\
[1, 3],\
[1, 4],\
[2, 5],\
[4, 6],\
[7, 8],\
[7, 9],\
[9, 10],\
[9, 11]])

data = BatchOperator.fromDataframe(df, schemaStr="source double, target double")
TreeDepthBatchOp()\
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

public class TreeDepthBatchOpTest extends AlinkTestBase {

	@Test
	public void testTreeDepth() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0.0, 1.0),
			Row.of(0.0, 2.0),
			Row.of(1.0, 3.0),
			Row.of(1.0, 4.0),
			Row.of(2.0, 5.0),
			Row.of(4.0, 6.0),
			Row.of(7.0, 8.0),
			Row.of(7.0, 9.0),
			Row.of(9.0, 10.0),
			Row.of(9.0, 11.0)
		};
		BatchOperator inData = new MemSourceBatchOp(Arrays.asList(rows), "source double,target double");

		TreeDepthBatchOp op = new TreeDepthBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(inData);
		op.print();
	}
}
```

### 运行结果

vertices|root|treeDepth
--------|----|---------
0.0000|0.0000|0.0000
4.0000|0.0000|2.0000
6.0000|0.0000|3.0000
1.0000|0.0000|1.0000
2.0000|0.0000|1.0000
3.0000|0.0000|2.0000
5.0000|0.0000|2.0000
7.0000|7.0000|0.0000
8.0000|7.0000|1.0000
9.0000|7.0000|1.0000
11.0000|7.0000|2.0000
10.0000|7.0000|2.0000
