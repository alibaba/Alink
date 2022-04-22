# Word2Vec预测 (Word2VecPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.nlp.Word2VecPredictStreamOp

Python 类名：Word2VecPredictStreamOp


## 功能介绍

Word2Vec是Google在2013年开源的一个将词表转为向量的算法，其利用神经网络，可以通过训练，将词映射到K维度空间向量，甚至对于表示词的向量进行操作还能和语义相对应，由于其简单和高效引起了很多人的关注。

Word2Vec的工具包相关链接：[https://code.google.com/p/word2vec/](https://code.google.com/p/word2vec/)

预测是根据word2vec的结果和文档的分词结果，将文档转成向量，向量维数保持与词的维数一致，同时每个维度通过对文档中的词求平均或者最大或者最小取得。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| predMethod | 向量组合方法 | 预测文档向量时，需要用到的方法。支持三种方法：平均（avg），最小（min）和最大（max），默认值为平均 | String |  | "AVG", "SUM", "MIN", "MAX" | "AVG" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  |  | " " |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |



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

public class Word2VecPredictStreamOpTest {
	@Test
	public void testWord2VecPredictStreamOp() throws Exception {
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
C|0.7310850737482761 0.8314776897399809 0.24063859340592783 0.60638116694726
B|0.7307931808428079 0.1004040760012987 0.4100461085450439 0.40737703647812473
A|0.7310290783979052 0.2932944253292183 0.9013769146172511 0.004089307575571348

#### 批预测结果
tokens|
------|
0.7309691109963297 0.40839206369016595 0.5173538721894075 0.3392825036669853|

#### 流预测结果
tokens|
------|
0.7309699600889779 0.40844142102496717 0.5173214287840606 0.3393146570468293|
