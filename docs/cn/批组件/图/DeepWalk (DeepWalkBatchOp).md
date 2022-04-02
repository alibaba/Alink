# DeepWalk (DeepWalkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.DeepWalkBatchOp

Python 类名：DeepWalkBatchOp


## 功能介绍

DeepWalk是2014年提出的一个新的方法，用来为网络中的结点学习隐式特征表达，即将网络中的每一个点表示成连续特征空间中的一个点向量。DeepWalk是无监督特征学习方法，利用随机游走(Random Walk)及语言模型(Language modeling)，学习出的隐式特征能够捕捉到网络的结构信息。后续论文也提出了一些扩展，如结合损失函数进行有监督学习、结合文本信息等等

[DeepWalk: Online Learning of Social Representations](http://www.perozzi.net/publications/14_kdd_deepwalk.pdf)

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| sourceCol | 起始点列名 | 用来指定起始点列 | String | ✓ |  |
| targetCol | 中止点点列名 | 用来指定中止点列 | String | ✓ |  |
| walkLength | 游走的长度 | 随机游走完向量的长度 | Integer | ✓ |  |
| walkNum | 路径数目 | 每一个起始点游走出多少条路径 | Integer | ✓ |  |
| alpha | 学习率 | 学习率 | Double |  | 0.025 |
| batchSize | batch大小 | batch大小, 按行计算 | Integer |  |  |
| isToUndigraph | 是否转无向图 | 选为true时，会将当前图转成无向图，然后再游走 | Boolean |  | false |
| minCount | 最小词频 | 最小词频 | Integer |  | 5 |
| negative | 负采样大小 | 负采样大小 | Integer |  | 5 |
| numIter | 迭代次数 | 迭代次数，默认为1。 | Integer |  | 1 |
| randomWindow | 是否使用随机窗口 | 是否使用随机窗口，默认使用 | String |  | "true" |
| vectorSize | embedding的向量长度 | embedding的向量长度 | Integer |  | 100 |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| window | 窗口大小 | 窗口大小 | Integer |  | 5 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
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
source = BatchOperator.fromDataframe(df, schemaStr="start string, end string, value double")

deepWalkBatchOp = DeepWalkBatchOp() \
  .setSourceCol("start")            \
  .setTargetCol("end")              \
  .setWeightCol("value")            \
  .setWalkNum(2)                    \
  .setWalkLength(2)                 \
  .setMinCount(1)                   \
  .setVectorSize(4)
deepWalkBatchOp.linkFrom(source).print()
```
### 运行结果

| node  | vec                                                                                |
|-------|------------------------------------------------------------------------------------|
| Karry | 0.03438692167401314,-0.04779096320271492,0.012648836709558964,-0.09576538950204849 |
| Lisa  | 0.11595723778009415,-0.08507091552019119,0.1099027618765831,0.013517010025680065   |
| Bella | 0.05783883109688759,0.08286115527153015,-0.06497485190629959,0.026532595977187157  |
| Alice | 0.05775630846619606,-0.099935382604599,-0.022451162338256836,-0.023144230246543884 |
| Lucy  | 0.11699658632278442,0.05271214246749878,-0.12347490340471268,-0.08684996515512466  |
| Bob   | -0.07306862622499466,-0.11596906185150146,-0.04183155298233032,0.03973118215799332 |


