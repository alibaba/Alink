# 共同邻居计算 (CommonNeighborsBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.CommonNeighborsBatchOp

Python 类名：CommonNeighborsBatchOp


## 功能介绍

共同邻居算法（Common Neighbors）是一种常用的基本图分析算法，可以计算两个节点所共有的邻居节点，
发现社交场合中的共同好友、以及在消费领域共同感兴趣的商品，进一步推测两个节点之间的潜在关系和相近程度。
适用于电商、社交等多种领域。

算法的输出有6列，前两列分别为两个节点的值，后面四列分别为共同邻居列表、共同邻居数量、Jaccard分数和Adamic分数。
Jaccard距离为 CommonNeighbors(a, b) / (size(a) + size(b) - CommonNeighbors(a,b))
Adamic Adar距离，首先计算每个节点的权重为 1/log(当前节点的邻居数量), Adamic Adar距离为两个节点的共同邻居的权重之和。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| isBipartiteGraph | 是否二部图 | 是否二部图 | Boolean |  |  | false |
| needTransformID | Not available! | Not available! | Boolean |  |  | true |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([["a1", "11L"],\
        ["a1", "12L"],\
        ["a1", "16L"],\
        ["a2", "11L"],\
        ["a2", "12L"],\
        ["a3", "12L"],\
        ["a3", "13L"]])

data = BatchOperator.fromDataframe(df, schemaStr="source string, target string")
CommonNeighborsBatchOp()\
    .setEdgeSourceCol("source")\
    .setEdgeTargetCol("target")\
    .setIsBipartiteGraph(False)\
    .linkFrom(data)\
    .print()
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

public class CommonNeighborsBatchOpTest {

	@Test
	public void testGraph() throws Exception {
		List<Row> rows = Arrays.asList(
			Row.of("a1", "11L"),
			Row.of("a1", "12L"),
			Row.of("a1", "16L"),
			Row.of("a2", "11L"),
			Row.of("a2", "12L"),
			Row.of("a3", "12L"),
			Row.of("a3", "13L")
		);

		BatchOperator inputdata = new MemSourceBatchOp(rows, "source string,target string");

		new CommonNeighborsBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target")
			.setIsBipartiteGraph(false)
			.linkFrom(inputdata)
			.print();
	}
}
```

### 运行结果

source|target|neighbors_list|cn |jaccards_score|adamic_score
------|------|--------------|---|--------------|------------
a2|a1|11L,12L|2|0.6667|2.3529
11L|12L|a2,a1|2|0.6667|2.3529
13L|12L|a3|1|0.3333|1.4427
16L|12L|a1|1|0.3333|0.9102
16L|11L|a1|1|0.5000|0.9102
a3|a2|12L|1|0.3333|0.9102
12L|11L|a1,a2|2|0.6667|2.3529
11L|16L|a1|1|0.5000|0.9102
12L|13L|a3|1|0.3333|1.4427
a2|a3|12L|1|0.3333|0.9102
a1|a3|12L|1|0.2500|0.9102
12L|16L|a1|1|0.3333|0.9102
a1|a2|11L,12L|2|0.6667|2.3529
a3|a1|12L|1|0.2500|0.9102


