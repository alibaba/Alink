# MetaPath游走 (MetaPathWalkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.MetaPathWalkBatchOp

Python 类名：MetaPathWalkBatchOp


## 功能介绍
MataPathWalk [1] 是描述随机游走的一种算法。在给定的图上，每次迭代过程中，点都会按照一定的metaPath转移到它的邻居上，转移到每个邻居的概率和连接这两个点的边的Type相关。通过这样的随机游走可以获得固定长度的随机游走序列，这可以类比自然语言中的句子。

[1] Dong et al. metapath2vec: Scalable Representation Learning for Heterogeneous Networks. KDD2017.

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| metaPath | 游走的模式 | 一般为用字符串表示，例如 "ABDFA" | String | ✓ |  |  |
| sourceCol | 起始点列名 | 用来指定起始点列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| targetCol | 中止点点列名 | 用来指定中止点列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| typeCol | 节点类型列名 | 用来指定节点类型列 | String | ✓ | 所选列类型为 [STRING] |  |
| vertexCol | 节点列名 | 用来指定节点列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| walkLength | 游走的长度 | 随机游走完向量的长度 | Integer | ✓ |  |  |
| walkNum | 路径数目 | 每一个起始点游走出多少条路径 | Integer | ✓ |  |  |
| delimiter | 分隔符 | 用来分割字符串 | String |  |  | " " |
| isToUndigraph | 是否转无向图 | 选为true时，会将当前图转成无向图，然后再游走 | Boolean |  |  | false |
| samplingMethod | 起始点列名 | 用来指定起始点列 | String |  |  | "ALIAS" |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1, 1, 1.0],
    [1, 2, 1.0],
    [2, 3, 1.0],
    [3, 4, 1.0],
    [4, 2, 1.0],
    [3, 1, 1.0],
    [2, 4, 1.0],
    [4, 1, 1.0]])

graph = BatchOperator.fromDataframe(df, schemaStr="start int, dest int, weight double")

df2 = pd.DataFrame([
    [1,"A"],
    [2,"B"],
    [3,"A"],
    [4,"B"]])

node = BatchOperator.fromDataframe(df2, schemaStr="node int, type string")

MetaPathWalkBatchOp() \
			.setWalkNum(10) \
			.setWalkLength(20) \
			.setSourceCol("start") \
			.setTargetCol("dest") \
			.setIsToUndigraph(True) \
			.setMetaPath("ABA") \
			.setVertexCol("node") \
			.setWeightCol("weight") \
			.setTypeCol("type").linkFrom(graph, node).print();
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.MetaPathWalkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MetaPathWalkBatchOpTest {
	@Test
	public void testMetaPathWalkBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, 1, 1.0),
			Row.of(1, 2, 1.0),
			Row.of(2, 3, 1.0),
			Row.of(3, 4, 1.0),
			Row.of(4, 2, 1.0),
			Row.of(3, 1, 1.0),
			Row.of(2, 4, 1.0)
		);
		BatchOperator <?> graph = new MemSourceBatchOp(df, "start int, dest int, weight double");
		List <Row> df2 = Arrays.asList(
			Row.of(1, "A"),
			Row.of(2, "B"),
			Row.of(3, "A")
		);
		BatchOperator <?> node = new MemSourceBatchOp(df2, "node int, type string");
		new MetaPathWalkBatchOp()
			.setWalkNum(10)
			.setWalkLength(20)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(true)
			.setMetaPath("ABA")
			.setVertexCol("node")
			.setWeightCol("weight")
			.setTypeCol("type").linkFrom(graph, node).print();
	}
}
```
### 运行结果
|path|
|----|
|1 4 3 2 3 2 1 2 1 2 1 2 1 2 1 2 3 2 3 2|
|1 4 3 2 1 4 3 2 3 4 3 2 1 4 1 4 1 2 3 2|
|1 2 3 4 3 2 1 4 3 4 3 4 1 2 3 2 3 4 3 2|
|3 4 3 4 1 4 1 4 1 2 1 2 1 2 3 2 1 4 3 2|
|1 4 1 2 3 2 1 2 3 2 3 2 3 2 1 2 1 4 3 2|
|3 2 3 4 3 2 3 2 3 4 1 2 3 2 1 2 1 4 3 2|
|3 4 3 4 1 2 3 4 1 4 1 4 3 2 3 4 3 2 1 2|
|3 4 1 4 1 4 3 4 1 2 1 2 1 4 1 4 3 2 1 2|
|1 2 1 4 3 4 3 2 3 2 1 2 1 2 1 2 3 2 1 2|
|3 2 1 2 3 2 3 4 3 2 1 4 3 4 1 2 1 2 1 2|
|1 2 1 4 1 4 1 2 1 2 3 2 3 4 1 2 1 2 1 4|
|1 2 1 4 3 2 1 2 1 4 1 4 1 4 1 2 1 2 1 2|
|1 4 1 4 1 2 1 2 1 4 1 2 3 2 1 2 1 2 1 2|
|3 4 3 4 3 2 1 2 3 2 3 4 1 2 1 2 1 2 1 4|
|3 2 3 4 3 4 1 4 3 2 3 4 3 2 3 2 3 4 1 4|
|3 2 3 2 3 4 3 2 1 2 1 4 1 2 1 2 3 4 1 4|
|1 4 3 4 1 2 1 4 1 2 3 2 1 2 1 2 3 4 1 4|
|3 4 3 2 3 4 1 2 1 4 3 4 3 2 1 4 1 4 1 2|
|3 4 3 4 1 2 3 2 1 4 1 4 3 2 1 4 1 4 1 4|
|1 4 1 2 3 2 3 4 1 2 1 2 1 2 3 2 1 4 1 2|


## 备注

- 上述表中的结点类型可以是任意类型。

- 给定metaPath 后算法按照 metaPath 去游走，直到路径长度达到walkLength。

- 给定的metaPath必须首尾两个结点的Type一样，正确的格式例子： “ABCA”，“VDEEV”，“ABA”等。

- 如果给定metaPath为“ABDSA”，walkLength为10，则游走得到的结果将是 “ABDSABDSAB”，达到10个结点是会截断。






