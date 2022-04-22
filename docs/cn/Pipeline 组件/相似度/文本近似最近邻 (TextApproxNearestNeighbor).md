# 文本近似最近邻 (TextApproxNearestNeighbor)
Java 类名：com.alibaba.alink.pipeline.similarity.TextApproxNearestNeighbor

Python 类名：TextApproxNearestNeighbor


## 功能介绍

文本相似度是在字符串相似度的基础上，基于词，计算两两文章或者句子之间的相似度，文章或者句子需要以空格分割的文本，计算方式和字符串相似度类似：支持SimHashHamming，MinHash和Jaccard三种近似相似度计算方式，通过选择metric参数可计算不同的相似度。

该功能由训练和预测组成，支持计算1. 求最近邻topN 2. 求radius范围内的邻居。该功能由预测时候的topN和radius参数控制, 如果填写了topN，则输出最近邻，如果填写了radius，则输出radius范围内的邻居。

SimhashHamming（SimHash_Hamming_Distance)相似度=1-距离/64.0，应选择metric的参数为SIMHASH_HAMMING_SIM。

MinHash应选择metric的参数为MINHASH_SIM。

Jaccard应选择metric的参数为JACCARD_SIM。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| idCol | id列名 | id列名 | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| metric | 距离类型 | 用于计算的距离类型 | String |  | "SIMHASH_HAMMING_SIM", "SIMHASH_HAMMING", "MINHASH_JACCARD_SIM", "JACCARD_SIM" | "SIMHASH_HAMMING_SIM" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numBucket | 分桶个数 | 分桶个数 | Integer |  |  | 10 |
| numHashTables | 哈希表个数 | 哈希表的数目 | Integer |  |  | 10 |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| radius | radius值 | radius值 | Double |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| seed | 采样种子 | 采样种子 | Long |  |  | 0 |
| topN | TopN的值 | TopN的值 | Integer |  | [1, +inf) | null |
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

pipeline = TextApproxNearestNeighbor().setIdCol("id").setSelectedCol("text1").setMetric("SIMHASH_HAMMING_SIM").setTopN(3)

pipeline.fit(inOp).transform(inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.similarity.TextApproxNearestNeighbor;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TextApproxNearestNeighborTest {
	@Test
	public void testTextApproxNearestNeighbor() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "a b c d e", "a a b c e"),
			Row.of(1, "a a c e d w", "a a b b e d"),
			Row.of(2, "c d e f a", "b b c e f a"),
			Row.of(3, "b d e f h", "d d e a c"),
			Row.of(4, "a c e d m", "a e e f b c")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, text1 string, text2 string");
		TextApproxNearestNeighbor pipeline = new TextApproxNearestNeighbor().setIdCol("id").setSelectedCol("text1")
			.setMetric("SIMHASH_HAMMING_SIM").setTopN(3);
		pipeline.fit(inOp).transform(inOp).print();
	}
}
```
### 运行结果
id|text1|text2
---|-----|-----
0|{"ID":"[0,1,4]","METRIC":"[1.0,0.96875,0.921875]"}|a a b c e
1|{"ID":"[1,0,4]","METRIC":"[1.0,0.96875,0.921875]"}|a a b b e d
2|{"ID":"[2,4,1]","METRIC":"[1.0,0.9375,0.890625]"}|b b c e f a
3|{"ID":"[3,4,2]","METRIC":"[1.0,0.828125,0.828125]"}|d d e a c
4|{"ID":"[4,2,1]","METRIC":"[1.0,0.9375,0.921875]"}|a e e f b c
