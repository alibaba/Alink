# Word2Vec训练 (Word2VecTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.nlp.Word2VecTrainBatchOp

Python 类名：Word2VecTrainBatchOp


## 功能介绍

Word2Vec是Google在2013年开源的一个将词表转为向量的算法，其利用神经网络，可以通过训练，将词映射到K维度空间向量，甚至对于表示词的向量进行操作还能和语义相对应，由于其简单和高效引起了很多人的关注。

Word2Vec的工具包相关链接：[https://code.google.com/p/word2vec/](https://code.google.com/p/word2vec/)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| alpha | 学习率 | 学习率 | Double |  |  | 0.025 |
| minCount | 最小词频 | 最小词频 | Integer |  |  | 5 |
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

df = pd.DataFrame([
    ["A B C"]
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='tokens string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='tokens string')
train = Word2VecTrainBatchOp().setSelectedCol("tokens").setMinCount(1).setVectorSize(4).linkFrom(inOp1)
predictBatch = Word2VecPredictBatchOp().setSelectedCol("tokens").linkFrom(train, inOp1)

train.lazyPrint(-1)
predictBatch.print()

predictStream = Word2VecPredictStreamOp(train).setSelectedCol("tokens").linkFrom(inOp2)
predictStream.print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.Word2VecPredictBatchOp;
import com.alibaba.alink.operator.batch.nlp.Word2VecTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.Word2VecPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class Word2VecTrainBatchOpTest {
	@Test
	public void testWord2VecTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("A B C")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "tokens string");
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "tokens string");
		BatchOperator <?> train = new Word2VecTrainBatchOp().setSelectedCol("tokens").setMinCount(1).setVectorSize(4)
			.linkFrom(inOp1);
		BatchOperator <?> predictBatch = new Word2VecPredictBatchOp().setSelectedCol("tokens").linkFrom(train, inOp1);
		train.lazyPrint(-1);
		predictBatch.print();
		StreamOperator <?> predictStream = new Word2VecPredictStreamOp(train).setSelectedCol("tokens").linkFrom(inOp2);
		predictStream.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
#### 模型结果
word|vec
----|---
A|0.7308596353097189 0.8314177144978963 0.24043567184236792 0.6063183430688116
C|0.7309068666584959 0.10053389527357781 0.41008241284786995 0.40747231850240395
B|0.7311470683768729 0.29342648043578945 0.9014165072579701 0.0041863689268244915

#### 批预测结果
tokens|
------|
0.7309711901150291 0.40845936340242117 0.5173115306494026 0.33932567683268|

#### 流预测结果
tokens|
------|
0.7309691109963297 0.4083920636901659 0.5173538721894075 0.3392825036669853|
