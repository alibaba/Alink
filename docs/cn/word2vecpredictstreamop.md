# word2vec流预测
## 功能介绍

Word2Vec是Google在2013年开源的一个将词表转为向量的算法，其利用神经网络，可以通过训练，将词映射到K维度空间向量，甚至对于表示词的向量进行操作还能和语义相对应，由于其简单和高效引起了很多人的关注。

Word2Vec的工具包相关链接：[https://code.google.com/p/word2vec/](https://code.google.com/p/word2vec/)

预测是根据word2vec的结果和文档的分词结果，将文档转成向量，向量维数保持与词的维数一致，同时每个维度通过对文档中的词求平均或者最大或者最小取得。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  | " " |
| predMethod | 向量组合方法 | 预测文档向量时，需要用到的方法。支持三种方法：平均（avg），最小（min）和最大（max），默认值为平均 | String |  | "AVG" |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 脚本示例
#### 脚本代码
```
import numpy as np
import pandas as pd

data = np.array([
    ["A B C"]
])

df = pd.DataFrame({"tokens": data[:, 0]})
inOp1 = dataframeToOperator(df, schemaStr='tokens string', op_type='batch')
inOp2 = dataframeToOperator(df, schemaStr='tokens string', op_type='stream')
train = Word2VecTrainBatchOp().setSelectedCol("tokens").setMinCount(1).setVectorSize(4).linkFrom(inOp1)
predictBatch = Word2VecPredictBatchOp().setSelectedCol("tokens").linkFrom(train, inOp1)

[model,predict] = collectToDataframes(train, predictBatch)
print(model)
print(predict)

predictStream = Word2VecPredictStreamOp(train).setSelectedCol("tokens").linkFrom(inOp2)
predictStream.print(refreshInterval=-1)
StreamOperator.execute()
```

#### 脚本运行结果
##### 模型结果
```
rowID word                                                vec
0    C  0.8955382525715048 0.7260255668945033 0.153084...
1    B  0.3799129268855519 0.09451568997723046 0.03543...
2    A  0.9284417086503712 0.7607143212094577 0.417053...
```

##### 预测结果
```
rowID    tokens
0  0.7346309627024759 0.5270851926937304 0.201858...
```
