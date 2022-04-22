# 文本最近邻预测 (TextNearestNeighborPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.similarity.TextNearestNeighborPredictBatchOp

Python 类名：TextNearestNeighborPredictBatchOp


## 功能介绍

文本相似度是在字符串相似度的基础上，基于词，计算两两文章或者句子之间的相似度，文章或者句子需要以空格分割的文本，计算方式和字符串相似度类似：支持Levenshtein Distance，Longest Common SubString，String Subsequence Kernel，Cosine三种精确相似度计算方式，通过选择metric参数可计算不同的相似度。

该功能由训练和预测组成，支持计算1. 求最近邻topN 2. 求radius范围内的邻居。该功能由预测时候的topN和radius参数控制, 如果填写了topN，则输出最近邻，如果填写了radius，则输出radius范围内的邻居。

Levenshtein（Levenshtein Distance）相似度=(1-距离)/length，length为两个字符长度的最大值离，应选metric的参数为LEVENSHTEIN_SIM。

LCS（Longest Common SubString）相似度=(1-距离)/length，length为两个字符长度的最大值，应选择metric的参数为LCS_SIM。

SSK（String Subsequence Kernel）支持相似度计算，应选择metric的参数为SSK。

Cosine（Cosine）支持相似度计算，应选择metric的参数为COSINE。
## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| radius | radius值 | radius值 | Double |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | [1, +inf) | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, "a b c d e", "a a b c e"],
    [1, "a a c e d w", "a a b b e d"],
    [2, "c d e f a", "b b c e f a"],
    [3, "b d e f h", "d d e a c"],
    [4, "a c e d m", "a e e f b c"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')

train = TextNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1").setMetric("LEVENSHTEIN_SIM").linkFrom(inOp)
predict = TextNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3).linkFrom(train, inOp)
predict.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.similarity.TextNearestNeighborPredictBatchOp;
import com.alibaba.alink.operator.batch.similarity.TextNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TextNearestNeighborPredictBatchOpTest {
	@Test
	public void testTextNearestNeighborPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "a b c d e", "a a b c e"),
			Row.of(1, "a a c e d w", "a a b b e d"),
			Row.of(2, "c d e f a", "b b c e f a"),
			Row.of(3, "b d e f h", "d d e a c"),
			Row.of(4, "a c e d m", "a e e f b c")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, text1 string, text2 string");
		BatchOperator <?> train = new TextNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1")
			.setMetric("LEVENSHTEIN_SIM").linkFrom(inOp);
		BatchOperator <?> predict =
			new TextNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3).linkFrom(
			train, inOp);
		predict.print();
	}
}
```
### 运行结果
id|text1|text2
---|-----|-----
0|a b c d e|{"ID":"[0,1,4]","METRIC":"[0.6,0.5,0.19999999999999996]"}
1|a a c e d w|{"ID":"[1,0,4]","METRIC":"[0.5,0.33333333333333337,0.33333333333333337]"}
2|c d e f a|{"ID":"[3,2,4]","METRIC":"[0.5,0.5,0.33333333333333337]"}
3|b d e f h|{"ID":"[3,2,4]","METRIC":"[0.4,0.4,0.19999999999999996]"}
4|a c e d m|{"ID":"[3,2,4]","METRIC":"[0.33333333333333337,0.33333333333333337,0.33333333333333337]"}



