# Word2Vec (Word2Vec)
Java 类名：com.alibaba.alink.pipeline.nlp.Word2Vec

Python 类名：Word2Vec


## 功能介绍

Word2Vec是Google在2013年开源的一个将词表转为向量的算法，其利用神经网络，可以通过训练，将词映射到K维度空间向量，甚至对于表示词的向量进行操作还能和语义相对应，由于其简单和高效引起了很多人的关注。

Word2Vec的工具包相关链接：[https://code.google.com/p/word2vec/](https://code.google.com/p/word2vec/)
## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| alpha | 学习率 | 学习率 | Double |  |  | 0.025 |
| minCount | 最小词频 | 最小词频 | Integer |  |  | 5 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numIter | 迭代次数 | 迭代次数，默认为1。 | Integer |  |  | 1 |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| predMethod | 向量组合方法 | 预测文档向量时，需要用到的方法。支持三种方法：平均（avg），最小（min）和最大（max），默认值为平均 | String |  | "AVG", "SUM", "MIN", "MAX" | "AVG" |
| randomWindow | 是否使用随机窗口 | 是否使用随机窗口，默认使用 | String |  |  | "true" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| vectorSize | embedding的向量长度 | embedding的向量长度 | Integer |  | x >= 1 | 100 |
| window | 窗口大小 | 窗口大小 | Integer |  |  | 5 |
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

inOp = BatchOperator.fromDataframe(df, schemaStr='tokens string')
word2vec = Word2Vec().setSelectedCol("tokens").setMinCount(1).setVectorSize(4)
word2vec.fit(inOp).transform(inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.nlp.Word2Vec;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class Word2VecTest {
	@Test
	public void testWord2Vec() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("A B C")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "tokens string");
		Word2Vec word2vec = new Word2Vec().setSelectedCol("tokens").setMinCount(1).setVectorSize(4);
		word2vec.fit(inOp).transform(inOp).print();
	}
}
```

### 运行结果
tokens|
------|
0.731000431888515 0.40841702428161525 0.5173676180773374 0.3393047625647364|
