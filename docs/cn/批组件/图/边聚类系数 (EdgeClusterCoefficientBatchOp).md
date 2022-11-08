# 边聚类系数 (EdgeClusterCoefficientBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.EdgeClusterCoefficientBatchOp

Python 类名：EdgeClusterCoefficientBatchOp


## 功能介绍
对于给定的图，对于图中的每条边，分别输出边的两个顶点的度，两个顶点的公共邻点数目，以及该边的聚类系数。
聚类系数的计算公式为 commonNeighbor / min(neighobr1, neighbor2)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([[1.0, 2.0],\
[1.0, 3.0],\
[3.0, 2.0],\
[5.0, 2.0],\
[3.0, 4.0],\
[4.0, 2.0],\
[5.0, 4.0],\
[5.0, 1.0],\
[5.0, 3.0],\
[5.0, 6.0],\
[5.0, 8.0],\
[7.0, 6.0],\
[7.0, 1.0],\
[7.0, 5.0],\
[8.0, 6.0],\
[8.0, 4.0]])

data = BatchOperator.fromDataframe(df, schemaStr="source double, target double")
EdgeClusterCoefficientBatchOp()\
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

public class EdgeClusterCoefficientBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1.0, 2.0),
			Row.of(1.0, 3.0),
			Row.of(3.0, 2.0),
			Row.of(5.0, 2.0),
			Row.of(3.0, 4.0),
			Row.of(4.0, 2.0),
			Row.of(5.0, 4.0),
			Row.of(5.0, 1.0),
			Row.of(5.0, 3.0),
			Row.of(5.0, 6.0),
			Row.of(5.0, 8.0),
			Row.of(7.0, 6.0),
			Row.of(7.0, 1.0),
			Row.of(7.0, 5.0),
			Row.of(8.0, 6.0),
			Row.of(8.0, 4.0)
		};
		MemSourceBatchOp dataSource = new MemSourceBatchOp(Arrays.asList(rows), "source double,target double");
		BatchOperator res = new EdgeClusterCoefficientBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(dataSource);
		res.print();
	}
}
```

### 运行结果


ndoe1|node2|neighbor1|neighbor2|commonNeighbor|edgeClusterCoefficient
-----|-----|---------|---------|--------------|----------------------
7.0000|1.0000|3|4|1|0.3333
3.0000|1.0000|4|4|2|0.5000
6.0000|7.0000|3|3|1|0.3333
7.0000|5.0000|3|7|2|0.6667
4.0000|5.0000|4|7|3|0.7500
3.0000|5.0000|4|7|3|0.7500
1.0000|5.0000|4|7|3|0.7500
6.0000|5.0000|3|7|2|0.6667
8.0000|5.0000|3|7|2|0.6667
2.0000|5.0000|4|7|3|0.7500
3.0000|4.0000|4|4|2|0.5000
4.0000|8.0000|4|3|1|0.3333
6.0000|8.0000|3|3|1|0.3333
4.0000|2.0000|4|4|2|0.5000
3.0000|2.0000|4|4|3|0.7500
1.0000|2.0000|4|4|2|0.5000
