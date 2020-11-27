## 功能介绍

文章相似度是在字符串相似度的基础上，基于词，计算两两文章或者句子之间的相似度，文章或者句子需要以空格分割的文本，计算方式和字符串相似度类似： 支持Levenshtein Distance，Longest Common SubString，String Subsequence Kernel，Cosine，SimHashHamming，MinHash和Jaccard七种相似度计算方式，通过选择metric参数可计算不同的相似度。

Levenshtein（Levenshtein Distance）支持距离和相似度两种方式，相似度=(1-距离)/length，length为两个字符长度的最大值，距离应选择metric的参数为LEVENSHTEIN，相似度应选metric的参数为LEVENSHTEIN_SIM。

LCS（Longest Common SubString）支持距离和相似度两种参数，相似度=(1-距离)/length，length为两个字符长度的最大值，距离应选择metric的参数为LCS，相似度应选择metric的参数为LCS_SIM。

SSK（String Subsequence Kernel）支持相似度计算，应选择metric的参数为SSK。

Cosine（Cosine）支持相似度计算，应选择metric的参数为COSINE。

SimhashHamming（SimHash_Hamming_Distance)，支持距离和相似度两种方式，相似度=1-距离/64.0，距离应选择metric的参数为SIMHASH_HAMMING，相似度应选择metric的参数为SIMHASH_HAMMING_SIM。

MinHash 支持相似度计算，应选择metric的参数为MINHASH_SIM。

Jaccard 支持相似度计算，应选择metric的参数为JACCARD_SIM。

Alink上文本相似度算法包括Batch组件和Stream组件。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| lambda | 匹配字符权重 | 匹配字符权重，SSK中使用 | Double |  | 0.5 |
| metric | 度量类型 | 计算距离时，可以取不同的度量 | String |  | "LEVENSHTEIN_SIM" |
| windowSize | 窗口大小 | 窗口大小 | Integer |  | 2 |
| numBucket | 分桶个数 | 分桶个数 | Integer |  | 10 |
| numHashTables | 哈希表个数 | 哈希表的数目 | Integer |  | 10 |
| seed | 采样种子 | 采样种子 | Long |  | 0 |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
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
inOp1 = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='batch')
inOp2 = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='stream')

op = TextSimilarityPairwiseBatchOp().setSelectedCols(["text1", "text2"]).setMetric("LEVENSHTEIN").setOutputCol("output")
op.linkFrom(inOp1).print()

op = TextSimilarityPairwiseStreamOp().setSelectedCols(["text1", "text2"]).setMetric("COSINE").setOutputCol("output")
op.linkFrom(inOp2).print()
StreamOperator.execute()
```
#### 脚本运行结果
id|text1|text2|output
---|-----|-----|------
0|a b c d e|a a b c e|2.0
1|a a c e d w|a a b b e d|3.0
2|c d e f a|b b c e f a|3.0
3|b d e f h|d d e a c|3.0
4|a c e d m|a e e f b c|4.0



