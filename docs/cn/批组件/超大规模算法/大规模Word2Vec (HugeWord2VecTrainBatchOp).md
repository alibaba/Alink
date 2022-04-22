# 大规模Word2Vec (HugeWord2VecTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.huge.HugeWord2VecTrainBatchOp

Python 类名：HugeWord2VecTrainBatchOp


## 功能介绍

Word2Vec是Google在2013年开源的一个将词表转为向量的算法，其利用神经网络，可以通过训练，将词映射到K维度空间向量，甚至对于表示词的向量进行操作还能和语义相对应，由于其简单和高效引起了很多人的关注。

Word2Vec的工具包相关链接：[https://code.google.com/p/word2vec/](https://code.google.com/p/word2vec/)

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 计算列对应的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| alpha | 学习率 | 学习率 | Double |  |  | 0.025 |
| batchSize | batch大小 | batch大小, 按行计算 | Integer |  | [1, +inf) |  |
| minCount | 最小词频 | 最小词频 | Integer |  |  | 5 |
| negative | 负采样大小 | 负采样大小 | Integer |  |  | 5 |
| numCheckpoint | checkPoint 数目 | checkPoint 数目 | Integer |  |  | 1 |
| numIter | 迭代次数 | 迭代次数，默认为1。 | Integer |  |  | 1 |
| randomWindow | 是否使用随机窗口 | 是否使用随机窗口，默认使用 | String |  |  | "true" |
| vectorSize | embedding的向量长度 | embedding的向量长度 | Integer |  | [1, +inf) | 100 |
| window | 窗口大小 | 窗口大小 | Integer |  |  | 5 |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  |  | " " |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

tokens = pd.DataFrame([
    ["A B C"]
])

source = BatchOperator.fromDataframe(tokens, schemaStr='tokens string')

word2vecBatchOp = HugeWord2VecTrainBatchOp() \
  .setSelectedCol("tokens")         \
  .setMinCount(1)                   \
  .setVectorSize(4)
word2vecBatchOp.linkFrom(source).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.huge.HugeWord2VecTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeWord2VecTrainBatchOpTest {
	@Test
	public void testHugeWord2VecTrainBatchOp() throws Exception {
		List <Row> tokens = Arrays.asList(
			Row.of("A B C")
		);
		BatchOperator <?> source = new MemSourceBatchOp(tokens, "tokens string");
		BatchOperator <?> word2vecBatchOp = new HugeWord2VecTrainBatchOp()
			.setSelectedCol("tokens")
			.setMinCount(1)
			.setVectorSize(4);
		word2vecBatchOp.linkFrom(source).print();
	}
}
```

### 运行结果

| word | vec                                                                                  |
|------|--------------------------------------------------------------------------------------|
| A    | 0.024366257712244987,0.07037621736526489,-0.04168345779180527,-0.06180821731686592   |
| B    | 0.05771925672888756,0.08288027346134186,-0.06486544758081436,0.026565641164779663    |
| C    | 0.034414440393447876,-0.047638311982154846,0.012538374401628971,-0.09579437971115112 |


## 备注

如果不输入vecTable的情况下是随机初始化，可能会造成两次结果相同的item的embedding结果绝对值差别比较大，请注意
