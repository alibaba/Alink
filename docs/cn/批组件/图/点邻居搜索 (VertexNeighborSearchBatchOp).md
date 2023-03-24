# 点邻居搜索 (VertexNeighborSearchBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.VertexNeighborSearchBatchOp

Python 类名：VertexNeighborSearchBatchOp


## 功能介绍

节点k度邻居子图查询算法，根据用户输入的图数据和一组节点，在图中查找它们的k度邻居，然后导出子图。在子图较小时，还提供图可视化功能。

## 参数说明

该组件至少接入一个输入桩，表示图的边集；可选接入第二个输入桩，表示图的点集。组件输出对应于输入桩，分别表示子图的边集和点集，其中点集在第二个输入桩有输入时才有输出。

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| sources | 起始节点集合 | 起始节点集合 | String[] | ✓ |  |  |
| asUndirectedGraph | 是否为无向图 | 是否为无向图 | Boolean |  |  | true |
| depth | 深度 | 寻找邻居的深度 | Integer |  |  | 1 |
| vertexIdCol | 节点ID列 | 表示节点ID的列名 | String |  |  | "id" |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["Alice", "Lisa", 1., "hello"],
		["Lisa", "Alice", 1., "hello"],
		["Lisa", "Karry", 1., "hello"],
		["Karry", "Lisa", 1., "213"],
		["Karry", "Bella", 1., "hello"],
		["Bella", "Karry", 1., "h123ello"],
		["Bella", "Lucy", 1., "hello"],
		["Lucy", "Bella", 1., "hello"],
		["Lucy", "Bob", 1., "123"],
		["Bob", "Lucy", 1., "hello"],
		["John", "Bob", 1., "hello"],
		["Bob", "John", 1., "hello"],
		["John", "Stella", 1., "123"],
		["Stella", "John", 1., "hello"],
		["Kate", "Stella", 1., "hello"],
		["Stella", "Kate", 1., "hello"],
		["Kate", "Jack", 1., "13"],
		["Jack", "Kate", 1., "hello"],
		["Jess", "Jack", 1., "13"],
		["Jack", "Jess", 1., "hello"],
		["Jess", "Jacob", 1., "hello"],
		["Jacob", "Jess", 1., "123"]]
)
data = BatchOperator.fromDataframe(df, schemaStr="start string, end string, value double, attr string")
op = VertexNeighborSearchBatchOp() \
    .setDepth(1) \
    .setSources(["John", "Lisa"]) \
    .setEdgeSourceCol("start") \
    .setEdgeTargetCol("end") \
    .setVertexIdCol("name") \
    .setAsUndirectedGraph(False)
op.linkFrom(data).print()
```

### Java 代码
```Java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VertexNeighborSearchBatchOpTest {
	@Test
	public void testDemo() throws Exception {
		List <Row> edgesRows = Arrays.asList(
			Row.of("Alice", "Lisa", 1., "hello"),
			Row.of("Lisa", "Alice", 1., "hello"),
			Row.of("Lisa", "Karry", 1., "hello"),
			Row.of("Karry", "Lisa", 1., "213"),
			Row.of("Karry", "Bella", 1., "hello"),
			Row.of("Bella", "Karry", 1., "h123ello"),
			Row.of("Bella", "Lucy", 1., "hello"),
			Row.of("Lucy", "Bella", 1., "hello"),
			Row.of("Lucy", "Bob", 1., "123"),
			Row.of("Bob", "Lucy", 1., "hello"),
			Row.of("John", "Bob", 1., "hello"),
			Row.of("Bob", "John", 1., "hello"),
			Row.of("John", "Stella", 1., "123"),
			Row.of("Stella", "John", 1., "hello"),
			Row.of("Kate", "Stella", 1., "hello"),
			Row.of("Stella", "Kate", 1., "hello"),
			Row.of("Kate", "Jack", 1., "13"),
			Row.of("Jack", "Kate", 1., "hello"),
			Row.of("Jess", "Jack", 1., "13"),
			Row.of("Jack", "Jess", 1., "hello"),
			Row.of("Jess", "Jacob", 1., "hello"),
			Row.of("Jacob", "Jess", 1., "123")
		);

		MemSourceBatchOp edgesSource = new MemSourceBatchOp(edgesRows,
			"start string, end string, value double, attr string");
		VertexNeighborSearchBatchOp op = new VertexNeighborSearchBatchOp()
			.setDepth(1)
			.setSources("John", "Lisa")
			.setEdgeSourceCol("start")
			.setEdgeTargetCol("end")
			.setVertexIdCol("name")
			.setAsUndirectedGraph(false);
		op.linkFrom(edgesSource).print();
	}
}
```

### 运行结果

start|end|value|attr
-----|---|-----|----
Alice|Lisa|1.0000|hello
Lisa|Karry|1.0000|hello
John|Bob|1.0000|hello
John|Stella|1.0000|123
Lisa|Alice|1.0000|hello
Karry|Lisa|1.0000|213
Bob|John|1.0000|hello
Stella|John|1.0000|hello
