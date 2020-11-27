## 功能介绍

本算法支持Levenshtein Distance，Longest Common SubString，String Subsequence Kernel，Cosine四种相似度精确计算方式，通过选择metric参数可计算不同的相似度。

该功能由训练和预测组成，支持计算1. 求最近邻topN 2. 求radius范围内的邻居。该功能由预测时候的topN和radius参数控制, 如果填写了topN，则输出最近邻，如果填写了radius，则输出radius范围内的邻居。

Levenshtein（Levenshtein Distance), 相似度=(1-距离)/length，length为两个字符长度的最大值，应选metric的参数为LEVENSHTEIN_SIM。

LCS（Longest Common SubString), 相似度=(1-距离)/length，length为两个字符长度的最大值，应选择metric的参数为LCS_SIM。

SSK（String Subsequence Kernel）支持相似度计算，应选择metric的参数为SSK。

Cosine（Cosine）支持相似度计算，应选择metric的参数为COSINE。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| metric | 距离类型 | 用于计算的距离类型 | String |  | "LEVENSHTEIN_SIM" |
| radius | radius值 | radius值 | Double |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | null |
| lambda | 匹配字符权重 | 匹配字符权重，SSK中使用 | Double |  | 0.5 |
| idCol | id列名 | id列名 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| windowSize | 窗口大小 | 窗口大小 | Integer |  | 2 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |



## 脚本示例
#### 脚本代码
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "abcde", "aabce"],
    [1, "aacedw", "aabbed"],
    [2, "cdefa", "bbcefa"],
    [3, "bdefh", "ddeac"],
    [4, "acedm", "aeefbc"]
])
df = pd.DataFrame({"id": data[:, 0], "text1": data[:, 1], "text2": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='batch')

pipeline = StringNearestNeighbor().setIdCol("id").setSelectedCol("text1").setMetric("LEVENSHTEIN_SIM").setTopN(3)

pipeline.fit(inOp).transform(inOp).print()
```
#### 脚本运行结果
   id   text1                                              text2
   
0   0   abcde  {"ID":"[0,1,4]","METRIC":"[0.6,0.5,0.199999999...

1   1  aacedw  {"ID":"[1,4,0]","METRIC":"[0.5,0.3333333333333...

2   2   cdefa  {"ID":"[2,3,1]","METRIC":"[0.5,0.5,0.333333333...

3   3   bdefh  {"ID":"[2,3,4]","METRIC":"[0.4,0.4,0.199999999...

4   4   acedm  {"ID":"[2,4,3]","METRIC":"[0.33333333333333337...



