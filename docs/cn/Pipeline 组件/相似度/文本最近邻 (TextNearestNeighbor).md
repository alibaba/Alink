# 文本最近邻 (TextNearestNeighbor)
Java 类名：com.alibaba.alink.pipeline.similarity.TextNearestNeighbor

Python 类名：TextNearestNeighbor


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
| idCol | id列名 | id列名 | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| lambda | 匹配字符权重 | 匹配字符权重，SSK中使用 | Double |  |  | 0.5 |
| metric | 距离类型 | 用于计算的距离类型 | String |  | "LEVENSHTEIN_SIM", "LEVENSHTEIN", "LCS_SIM", "LCS", "SSK", "COSINE" | "LEVENSHTEIN_SIM" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| radius | radius值 | radius值 | Double |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | x >= 1 | null |
| windowSize | 窗口大小 | 窗口大小 | Integer |  | x >= 1 | 2 |
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
    [0, "a b c d e", "a a b c e"],
    [1, "a a c e d w", "a a b b e d"],
    [2, "c d e f a", "b b c e f a"],
    [3, "b d e f h", "d d e a c"],
    [4, "a c e d m", "a e e f b c"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')

pipeline = TextNearestNeighbor().setIdCol("id").setSelectedCol("text1").setMetric("LEVENSHTEIN_SIM").setTopN(3)

pipeline.fit(inOp).transform(inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.similarity.TextNearestNeighbor;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TextNearestNeighborTest {
	@Test
	public void testTextNearestNeighbor() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "a b c d e", "a a b c e"),
			Row.of(1, "a a c e d w", "a a b b e d"),
			Row.of(2, "c d e f a", "b b c e f a"),
			Row.of(3, "b d e f h", "d d e a c"),
			Row.of(4, "a c e d m", "a e e f b c")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, text1 string, text2 string");
		TextNearestNeighbor pipeline = new TextNearestNeighbor().setIdCol("id").setSelectedCol("text1").setMetric(
			"LEVENSHTEIN_SIM").setTopN(3);
		pipeline.fit(inOp).transform(inOp).print();
	}
}
```
### 运行结果
id|text1|text2
---|-----|-----
0|{"ID":"[0,1,4]","METRIC":"[1.0,0.5,0.4]"}|a a b c e
1|{"ID":"[1,4,0]","METRIC":"[1.0,0.6666666666666667,0.5]"}|a a b b e d
2|{"ID":"[2,3,4]","METRIC":"[1.0,0.6,0.19999999999999996]"}|b b c e f a
3|{"ID":"[3,2,4]","METRIC":"[1.0,0.6,0.19999999999999996]"}|d d e a c
4|{"ID":"[4,1,0]","METRIC":"[1.0,0.6666666666666667,0.4]"}|a e e f b c



