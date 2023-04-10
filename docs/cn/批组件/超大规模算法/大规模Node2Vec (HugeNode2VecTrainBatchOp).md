# 大规模Node2Vec (HugeNode2VecTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.huge.HugeNode2VecTrainBatchOp

Python 类名：HugeNode2VecTrainBatchOp


## 功能介绍

node2vec是一种用于网络中的特征学习有效的可扩展算法，该算法可以使用SGD有效地优化，能根据网络中的既定原则，为发现符合不同等值的表示提供了灵活性

[node2vec: Scalable Feature Learning for Networks](https://cs.stanford.edu/~jure/pubs/node2vec-kdd16.pdf)

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| sourceCol | 起始点列名 | 用来指定起始点列 | String | ✓ |  |  |
| targetCol | 中止点点列名 | 用来指定中止点列 | String | ✓ |  |  |
| walkLength | 游走的长度 | 随机游走完向量的长度 | Integer | ✓ |  |  |
| walkNum | 路径数目 | 每一个起始点游走出多少条路径 | Integer | ✓ |  |  |
| alpha | 学习率 | 学习率 | Double |  |  | 0.025 |
| batchSize | batch大小 | batch大小, 按行计算 | Integer |  | x >= 1 |  |
| isToUndigraph | 是否转无向图 | 选为true时，会将当前图转成无向图，然后再游走 | Boolean |  |  | false |
| minCount | 最小词频 | 最小词频 | Integer |  |  | 5 |
| negative | 负采样大小 | 负采样大小 | Integer |  |  | 5 |
| numCheckpoint | checkPoint 数目 | checkPoint 数目 | Integer |  |  | 1 |
| numIter | 迭代次数 | 迭代次数，默认为1。 | Integer |  |  | 1 |
| p | p | p越小越趋向于访问到已经访问的节点，反之则趋向于访问没有访问过的节点 | Double |  |  | 1.0 |
| q | q | q>1时行为类似于bfs趋向于访问和访问过的节点相连的节点，q<1时行为类似于dfs | Double |  |  | 1.0 |
| randomWindow | 是否使用随机窗口 | 是否使用随机窗口，默认使用 | String |  |  | "true" |
| vectorSize | embedding的向量长度 | embedding的向量长度 | Integer |  | x >= 1 | 100 |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| window | 窗口大小 | 窗口大小 | Integer |  |  | 5 |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  |  | " " |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ["Bob", "Lucy", 1.],
    ["Lucy", "Bob", 1.],
    ["Lucy", "Bella", 1.],
    ["Bella", "Lucy", 1.],
    ["Alice", "Lisa", 1.],
    ["Lisa", "Alice", 1.],
    ["Lisa", "Karry", 1.],
    ["Karry", "Lisa", 1.],
    ["Karry", "Bella", 1.],
    ["Bella", "Karry", 1.]
])
source =  BatchOperator.fromDataframe(df_data, schemaStr='start string, end string, value double')

node2vecBatchOp = HugeNode2VecTrainBatchOp() \
  .setSourceCol("start")            \
  .setTargetCol("end")              \
  .setWeightCol("value")            \
  .setWalkNum(2)                    \
  .setWalkLength(2)                 \
  .setMinCount(1)                   \
  .setVectorSize(4)
node2vecBatchOp.linkFrom(source).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.huge.HugeNode2VecTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeNode2VecTrainBatchOpTest {
	@Test
	public void testHugeNode2VecTrainBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("Bob", "Lucy", 1.),
			Row.of("Lucy", "Bob", 1.),
			Row.of("Lucy", "Bella", 1.),
			Row.of("Bella", "Lucy", 1.),
			Row.of("Alice", "Lisa", 1.),
			Row.of("Lisa", "Alice", 1.),
			Row.of("Lisa", "Karry", 1.),
			Row.of("Karry", "Lisa", 1.),
			Row.of("Karry", "Bella", 1.),
			Row.of("Bella", "Karry", 1.)
		);
		BatchOperator <?> source = new MemSourceBatchOp(df_data, "start string, end string, value double");
		BatchOperator <?> node2vecBatchOp = new HugeNode2VecTrainBatchOp()
			.setSourceCol("start")
			.setTargetCol("end")
			.setWeightCol("value")
			.setWalkNum(2)
			.setWalkLength(2)
			.setMinCount(1)
			.setVectorSize(4);
		node2vecBatchOp.linkFrom(source).print();
	}
}
```

### 运行结果

| node  | vec                                                                                  |
|-------|--------------------------------------------------------------------------------------|
| Karry | 0.02435881271958351,0.0703350380063057,-0.04173225536942482,-0.06183897703886032     |
| Bella | -0.028720347210764885,0.02828666940331459,0.12123052030801773,0.12075022608041763    |
| Alice | 0.03435942903161049,-0.04773801192641258,0.0125938905403018,-0.09576953202486038     |
| Lisa  | -0.07306616753339767,-0.11595576256513596,-0.04181118682026863,0.03970039263367653   |
| Bob   | 0.0577755942940712,0.08282522112131119,-0.06487344205379486,0.026600968092679977     |
| Lucy  | 0.057738181203603745,-0.09987597167491913,-0.022486409172415733,-0.02312176302075386 |


