# 大规模MethPath2Vec (HugeMetaPath2VecTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.huge.HugeMetaPath2VecTrainBatchOp

Python 类名：HugeMetaPath2VecTrainBatchOp


## 功能介绍
沿着之前random walk的思路往前走，metapath2vec的方法提出了控制随机游走的模式，这样就可以在生成的序列上根据节点类型的不同来控制序列游走，这样也就可以对异质网络（Heterogeneous Networks）进行表征学习。在游走之前需要设定一个metapath，也就是游走时节点类型的模式

[metapath2vec: Scalable Representation Learning for Heterogeneous Networks](https://ericdongyx.github.io/papers/KDD17-dong-chawla-swami-metapath2vec.pdf)

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| metaPath | 游走的模式 | 一般为用字符串表示，例如 "ABDFA" | String | ✓ |  |
| sourceCol | 起始点列名 | 用来指定起始点列 | String | ✓ |  |
| targetCol | 中止点点列名 | 用来指定中止点列 | String | ✓ |  |
| typeCol | 节点类型列名 | 用来指定节点类型列 | String | ✓ |  |
| vertexCol | 节点列名 | 用来指定节点列 | String | ✓ |  |
| walkLength | 游走的长度 | 随机游走完向量的长度 | Integer | ✓ |  |
| walkNum | 路径数目 | 每一个起始点游走出多少条路径 | Integer | ✓ |  |
| alpha | 学习率 | 学习率 | Double |  | 0.025 |
| batchSize | batch大小 | batch大小, 按行计算 | Integer |  |  |
| isToUndigraph | 是否转无向图 | 选为true时，会将当前图转成无向图，然后再游走 | Boolean |  | false |
| minCount | 最小词频 | 最小词频 | Integer |  | 5 |
| mode | metapath中word2vec的模式，分别为metapath2vec和metapath2vecpp | metapath的模式 | String |  | "METAPATH2VEC" |
| negative | 负采样大小 | 负采样大小 | Integer |  | 5 |
| numCheckpoint | Not available! | Not available! | Integer |  | 1 |
| numIter | 迭代次数 | 迭代次数，默认为1。 | Integer |  | 1 |
| randomWindow | 是否使用随机窗口 | 是否使用随机窗口，默认使用 | String |  | "true" |
| vectorSize | embedding的向量长度 | embedding的向量长度 | Integer |  | 100 |
| weightCol | 权重列名 | 用来指定权重列, 权重列的值必须为非负的浮点数, 否则算法抛异常。 | String |  | null |
| window | 窗口大小 | 窗口大小 | Integer |  | 5 |



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
nodeType = pd.DataFrame([
    ["Bob", "A"],
    ["Bella", "A"],
    ["Karry", "A"],
    ["Lucy", "B"],
    ["Alice", "B"],
    ["Lisa", "B"],
    ["Karry", "B"]
])
type = BatchOperator.fromDataframe(nodeType, schemaStr='node string, type string')
metapathBatchOp = HugeMetaPath2VecTrainBatchOp() \
  .setSourceCol("start")            \
  .setTargetCol("end")              \
  .setWeightCol("value")            \
  .setVertexCol("node")             \
  .setTypeCol("type")               \
  .setMetaPath("ABA")               \
  .setWalkNum(2)                    \
  .setWalkLength(2)                 \
  .setMinCount(1)                   \
  .setVectorSize(4)
metapathBatchOp.linkFrom(source, type).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.huge.HugeMetaPath2VecTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeMetaPath2VecTrainBatchOpTest {
	@Test
	public void testHugeMetaPath2VecTrainBatchOp() throws Exception {
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
		List <Row> nodeType = Arrays.asList(
			Row.of("Bob", "A"),
			Row.of("Bella", "A"),
			Row.of("Karry", "A"),
			Row.of("Lucy", "B"),
			Row.of("Alice", "B"),
			Row.of("Lisa", "B"),
			Row.of("Karry", "B")
		);
		BatchOperator <?> type = new MemSourceBatchOp(nodeType, "node string, type string");
		BatchOperator <?> metapathBatchOp = new HugeMetaPath2VecTrainBatchOp()
			.setSourceCol("start")
			.setTargetCol("end")
			.setWeightCol("value")
			.setVertexCol("node")
			.setTypeCol("type")
			.setMetaPath("ABA")
			.setWalkNum(2)
			.setWalkLength(2)
			.setMinCount(1)
			.setVectorSize(4);
		metapathBatchOp.linkFrom(source, type).print();
	}
}
```

### 运行结果

node|vec
----|---
Karry|-0.028718041256070137,0.02825581468641758,0.12125638127326965,0.1207452341914177
Bella|0.03437831997871399,-0.0477546751499176,0.012570690363645554,-0.0958133116364479
Bob|0.024427175521850586,0.07044785469770432,-0.04175269603729248,-0.06182029843330383
Lucy|0.05776885524392128,0.08288335055112839,-0.06490718573331833,0.026563744992017746

