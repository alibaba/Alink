# 点聚类系数 (VertexClusterCoefficientBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.VertexClusterCoefficientBatchOp

Python 类名：VertexClusterCoefficientBatchOp


## 功能介绍
对于给定的图，计算图中的每个顶点，输出其邻居个数，其邻点间的边数，以及每个点的聚类系数。

## 参数说明

该组件有一个输入桩，表示图的边集。组件有一个输出桩，输出图中所有顶点，其邻点个数，其邻点间的边数，以及其对应的聚类系数。

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([[1, 2],
                   [1, 3],
                   [1, 4],
                   [1, 5],
                   [1, 6],
                   [2, 3],
                   [4, 3],
                   [5, 4],
                   [5, 6],
                   [5, 7],
                   [6, 7]])

data = BatchOperator.fromDataframe(df, schemaStr="source double, target double")

vertexClusterCoefficient = VertexClusterCoefficientBatchOp() \
    .setEdgeSourceCol("source") \
    .setEdgeTargetCol("target")
vertexClusterCoefficient.linkFrom(data).print()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

public class VertexClusterCoefficientBatchOpTest {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, 2),
			Row.of(1, 3),
			Row.of(1, 4),
			Row.of(1, 5),
			Row.of(1, 6),
			Row.of(2, 3),
			Row.of(4, 3),
			Row.of(5, 4),
			Row.of(5, 6),
			Row.of(5, 7),
			Row.of(6, 7)
		};
		BatchOperator <?> inData = new MemSourceBatchOp(rows, "source int, target int");
		VertexClusterCoefficientBatchOp op = new VertexClusterCoefficientBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.linkFrom(inData);
		op.print();
	}
}
```


### 运行结果

|vertexId|vertexDegree|edgeNum|coefficient|
|--------|------------|-------|-----------|
|2|2|1|1.0000|
|1|5|4|0.4000|
|7|2|1|1.0000|
|5|4|3|0.5000|
|3|3|2|0.6667|
|4|3|2|0.6667|
|6|3|2|0.6667|

