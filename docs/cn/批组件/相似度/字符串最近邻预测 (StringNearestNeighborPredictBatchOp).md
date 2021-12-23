# 字符串最近邻预测 (StringNearestNeighborPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.similarity.StringNearestNeighborPredictBatchOp

Python 类名：StringNearestNeighborPredictBatchOp


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
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| radius | radius值 | radius值 | Double |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, "abcde", "aabce"],
    [1, "aacedw", "aabbed"],
    [2, "cdefa", "bbcefa"],
    [3, "bdefh", "ddeac"],
    [4, "acedm", "aeefbc"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')

train = StringNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1").setMetric("LEVENSHTEIN_SIM").linkFrom(inOp)
predict = StringNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3).linkFrom(train, inOp)
predict.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.similarity.StringNearestNeighborPredictBatchOp;
import com.alibaba.alink.operator.batch.similarity.StringNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StringNearestNeighborPredictBatchOpTest {
	@Test
	public void testStringNearestNeighborPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "abcde", "aabce"),
			Row.of(1, "aacedw", "aabbed"),
			Row.of(2, "cdefa", "bbcefa"),
			Row.of(3, "bdefh", "ddeac"),
			Row.of(4, "acedm", "aeefbc")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, text1 string, text2 string");
		BatchOperator <?> train = new StringNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1")
			.setMetric("LEVENSHTEIN_SIM").linkFrom(inOp);
		BatchOperator <?> predict = new StringNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3)
			.linkFrom(train, inOp);
		predict.print();
	}
}
```
### 运行结果
id|text1|text2
---|-----|-----
0|abcde|{"ID":"[0,1,4]","METRIC":"[0.6,0.5,0.19999999999999996]"}
1|aacedw|{"ID":"[1,0,4]","METRIC":"[0.5,0.33333333333333337,0.33333333333333337]"}
2|cdefa|{"ID":"[2,3,1]","METRIC":"[0.5,0.5,0.33333333333333337]"}
3|bdefh|{"ID":"[2,3,4]","METRIC":"[0.4,0.4,0.19999999999999996]"}
4|acedm|{"ID":"[4,3,2]","METRIC":"[0.33333333333333337,0.33333333333333337,0.33333333333333337]"}

