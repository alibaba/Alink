## 功能介绍

Word2Vec是Google在2013年开源的一个将词表转为向量的算法，其利用神经网络，可以通过训练，将词映射到K维度空间向量，甚至对于表示词的向量进行操作还能和语义相对应，由于其简单和高效引起了很多人的关注。

Word2Vec的工具包相关链接：[https://code.google.com/p/word2vec/](https://code.google.com/p/word2vec/)
## 参数说明
<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| numIter | 迭代次数 | 迭代次数，默认为1。 | Integer |  | 1 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| vectorSize | embedding的向量长度 | embedding的向量长度 | Integer |  | 100 |
| alpha | 学习率 | 学习率 | Double |  | 0.025 |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  | " " |
| minCount | 最小词频 | 最小词频 | Integer |  | 5 |
| randomWindow | 是否使用随机窗口 | 是否使用随机窗口，默认使用 | String |  | "true" |
| window | 窗口大小 | 窗口大小 | Integer |  | 5 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  | " " |
| predMethod | 向量组合方法 | 预测文档向量时，需要用到的方法。支持三种方法：平均（avg），最小（min）和最大（max），默认值为平均 | String |  | "avg" |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码
```
import numpy as np
import pandas as pd

data = np.array([
    ["A B C"]
])

df = pd.DataFrame({"tokens": data[:, 0]})
inOp = dataframeToOperator(df, schemaStr='tokens string', op_type='batch')
word2vec = Word2Vec().setSelectedCol("tokens").setMinCount(1).setVectorSize(4)
word2vec.fit(inOp).transform(inOp).print()
```

#### 脚本运行结果
##### 预测结果
```
rowID    tokens
0  0.7346309627024759 0.5270851926937304 0.201858...
```