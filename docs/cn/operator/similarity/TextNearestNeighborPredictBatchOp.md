## 功能介绍

文本相似度是在字符串相似度的基础上，基于词，计算两两文章或者句子之间的相似度，文章或者句子需要以空格分割的文本，计算方式和字符串相似度类似：支持Levenshtein Distance，Longest Common SubString，String Subsequence Kernel，Cosine三种精确相似度计算方式，通过选择metric参数可计算不同的相似度。

该功能由训练和预测组成，支持计算1. 求最近邻topN 2. 求radius范围内的邻居。该功能由预测时候的topN和radius参数控制, 如果填写了topN，则输出最近邻，如果填写了radius，则输出radius范围内的邻居。

Levenshtein（Levenshtein Distance）相似度=(1-距离)/length，length为两个字符长度的最大值离，应选metric的参数为LEVENSHTEIN_SIM。

LCS（Longest Common SubString）相似度=(1-距离)/length，length为两个字符长度的最大值，应选择metric的参数为LCS_SIM。

SSK（String Subsequence Kernel）支持相似度计算，应选择metric的参数为SSK。

Cosine（Cosine）支持相似度计算，应选择metric的参数为COSINE。
## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| radius | radius值 | radius值 | Double |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | null |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |



## 脚本示例
#### 脚本代码
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "a b c d e", "a a b c e"],
    [1, "a a c e d w", "a a b b e d"],
    [2, "c d e f a", "b b c e f a"],
    [3, "b d e f h", "d d e a c"],
    [4, "a c e d m", "a e e f b c"]
])
df = pd.DataFrame({"id": data[:, 0], "text1": data[:, 1], "text2": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='batch')

train = TextNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1").setMetric("LEVENSHTEIN_SIM").linkFrom(inOp)
predict = TextNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3).linkFrom(train, inOp)
predict.print()
```
#### 脚本运行结果
   id   text1                                              text2
   
0   0   abcde  {"ID":"[0,1,4]","METRIC":"[0.6,0.5,0.199999999...

1   1  aacedw  {"ID":"[1,4,0]","METRIC":"[0.5,0.3333333333333...

2   2   cdefa  {"ID":"[2,3,1]","METRIC":"[0.5,0.5,0.333333333...

3   3   bdefh  {"ID":"[2,3,4]","METRIC":"[0.4,0.4,0.199999999...

4   4   acedm  {"ID":"[2,4,3]","METRIC":"[0.33333333333333337...




