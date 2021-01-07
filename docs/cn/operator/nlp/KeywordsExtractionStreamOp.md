## 功能介绍

关键词抽取是自然语言处理中的重要技术之一，具体是指从文本里面把跟这篇文章意义最相关的一些词抽取出来。

本算法基于TextRank，它受到网页之间关系PageRank算法启发，利用局部词汇之间关系（共现窗口）构建网络，计算词的重要性，选取权重大的作为关键词。

常用流程： 原始语料 → 分词 → 停用词过滤 → 关键词抽取


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| topN | 前N的数据 | 挑选最近的N个数据 | Integer |  | 10 |
| windowSize | 窗口大小 | 窗口大小 | Integer |  | 2 |
| dampingFactor | 阻尼系数 | 阻尼系数 | Double |  | 0.85 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | 100 |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| epsilon | 收敛阈值 | 收敛阈值 | Double |  | 1.0E-6 |




## 脚本示例
#### 脚本代码
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, u'二手旧书:医学电磁成像'],
    [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
    [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
    [3, u'二手中国糖尿病文献索引'],
    [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']])
df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, text string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, text string')

segment = SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1)
remover = StopWordsRemoverBatchOp().setSelectedCol("text").linkFrom(segment)
keywords = KeywordsExtractionBatchOp().setSelectedCol("text").setMethod("TF_IDF").setTopN(3).linkFrom(remover)
keywords.print()

segment = SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2)
remover = StopWordsRemoverStreamOp().setSelectedCol("text").linkFrom(segment)
keywords = KeywordsExtractionStreamOp().setSelectedCol("text").setTopN(3).linkFrom(remover)
keywords.print()
StreamOperator.execute()
```

#### 脚本运行结果
```
        text  id
0   电磁 旧书 成像   0
1  李宜燮 选读 下册   1
2   谢恩 象棋 入门   2
3   索引 中国 文献   3
4    十二册 版 全   4
```








