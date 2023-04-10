# 大规模带标签的Word2Vec (HugeLabeledWord2VecTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.huge.HugeLabeledWord2VecTrainBatchOp

Python 类名：HugeLabeledWord2VecTrainBatchOp


## 功能介绍
Word2Vec是Google在2013年开源的一个将词表转为向量的算法，其利用神经网络，可以通过训练，将词映射到K维度空间向量，甚至对于表示词的向量进行操作还能和语义相对应，由于其简单和高效引起了很多人的关注。

Google Word2Vec的工具包相关链接：[https://code.google.com/p/word2vec/](https://code.google.com/p/word2vec/)

支持metapath2vec++训练：[metapath2vec: Scalable Representation Learning forHeterogeneous Networks](https://ericdongyx.github.io/papers/KDD17-dong-chawla-swami-metapath2vec.pdf)

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 计算列对应的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| typeCol | 节点类型列名 | 用来指定节点类型列 | String | ✓ |  |  |
| vertexCol | 节点列名 | 用来指定节点列 | String | ✓ | 所选列类型为 [STRING] |  |
| alpha | 学习率 | 学习率 | Double |  |  | 0.025 |
| batchSize | batch大小 | batch大小, 按行计算 | Integer |  | x >= 1 |  |
| minCount | 最小词频 | 最小词频 | Integer |  |  | 5 |
| negative | 负采样大小 | 负采样大小 | Integer |  |  | 5 |
| numCheckpoint | checkPoint 数目 | checkPoint 数目 | Integer |  |  | 1 |
| numIter | 迭代次数 | 迭代次数，默认为1。 | Integer |  |  | 1 |
| randomWindow | 是否使用随机窗口 | 是否使用随机窗口，默认使用 | String |  |  | "true" |
| vectorSize | embedding的向量长度 | embedding的向量长度 | Integer |  | x >= 1 | 100 |
| window | 窗口大小 | 窗口大小 | Integer |  |  | 5 |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  |  | " " |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

tokens = pd.DataFrame([
    ["Bob Lucy Bella"]
])

nodeType = pd.DataFrame([
    ["Bob", "A"],
    ["Bella", "A"],
    ["Karry", "A"],
    ["Lucy", "B"],
    ["Alice", "B"],
    ["Lisa", "B"]
])

source = BatchOperator.fromDataframe(tokens, schemaStr='tokens string')
typed = BatchOperator.fromDataframe(nodeType, schemaStr='node string, type string')

labeledWord2vecBatchOp = HugeLabeledWord2VecTrainBatchOp() \
  .setSelectedCol("tokens")         \
  .setVertexCol("node")             \
  .setTypeCol("type")               \
  .setMinCount(1)                   \
  .setVectorSize(4)
labeledWord2vecBatchOp.linkFrom(source, typed).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.huge.HugeLabeledWord2VecTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeLabeledWord2VecTrainBatchOpTest {
	@Test
	public void testHugeLabeledWord2VecTrainBatchOp() throws Exception {
		List <Row> tokens = Arrays.asList(
			Row.of("Bob Lucy Bella")
		);
		List <Row> nodeType = Arrays.asList(
			Row.of("Bob", "A"),
			Row.of("Bella", "A"),
			Row.of("Karry", "A"),
			Row.of("Lucy", "B"),
			Row.of("Alice", "B"),
			Row.of("Lisa", "B")
		);
		BatchOperator <?> source = new MemSourceBatchOp(tokens, "tokens string");
		BatchOperator <?> typed = new MemSourceBatchOp(nodeType, "node string, type string");
		BatchOperator <?> labeledWord2vecBatchOp = new HugeLabeledWord2VecTrainBatchOp()
			.setSelectedCol("tokens")
			.setVertexCol("node")
			.setTypeCol("type")
			.setMinCount(1)
			.setVectorSize(4);
		labeledWord2vecBatchOp.linkFrom(source, typed).print();
	}
}
```
### 运行结果

| word  | vec                                                                                |
|-------|------------------------------------------------------------------------------------|
| Lucy  | 0.03437602147459984,-0.04761518910527229,0.012536839582026005,-0.09563367068767548 |
| Bob   | 0.057709891349077225,0.08290477842092514,-0.06487766653299332,0.026675613597035408 |
| Bella | 0.02439533919095993,0.07039660215377808,-0.04170553758740425,-0.061801809817552567 |

