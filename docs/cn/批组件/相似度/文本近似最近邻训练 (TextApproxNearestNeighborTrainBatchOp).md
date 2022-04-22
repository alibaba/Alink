# 文本近似最近邻训练 (TextApproxNearestNeighborTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.similarity.TextApproxNearestNeighborTrainBatchOp

Python 类名：TextApproxNearestNeighborTrainBatchOp


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
| numBucket | 分桶个数 | 分桶个数 | Integer |  |  | 10 |
| numHashTables | 哈希表个数 | 哈希表的数目 | Integer |  |  | 10 |
| seed | 采样种子 | 采样种子 | Long |  |  | 0 |



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

train = TextApproxNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1").setMetric("SIMHASH_HAMMING_SIM").linkFrom(inOp)
predict = TextApproxNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3).linkFrom(train, inOp)
predict.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.similarity.TextApproxNearestNeighborPredictBatchOp;
import com.alibaba.alink.operator.batch.similarity.TextApproxNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TextApproxNearestNeighborTrainBatchOpTest {
	@Test
	public void testTextApproxNearestNeighborTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "a b c d e", "a a b c e"),
			Row.of(1, "a a c e d w", "a a b b e d"),
			Row.of(2, "c d e f a", "b b c e f a"),
			Row.of(3, "b d e f h", "d d e a c"),
			Row.of(4, "a c e d m", "a e e f b c")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, text1 string, text2 string");
		BatchOperator <?> train = new TextApproxNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1")
			.setMetric("SIMHASH_HAMMING_SIM").linkFrom(inOp);
		BatchOperator <?> predict = new TextApproxNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3)
			.linkFrom(train, inOp);
		predict.print();
	}
}
```
### 运行结果
id|text1|text2
---|-----|-----
0|a b c d e|{"ID":"[0,1,2]","METRIC":"[0.953125,0.921875,0.90625]"}
1|a a c e d w|{"ID":"[0,1,4]","METRIC":"[0.9375,0.90625,0.859375]"}
2|c d e f a|{"ID":"[0,1,4]","METRIC":"[0.890625,0.859375,0.8125]"}
3|b d e f h|{"ID":"[4,2,1]","METRIC":"[0.9375,0.90625,0.890625]"}
4|a c e d m|{"ID":"[1,0,4]","METRIC":"[0.921875,0.921875,0.90625]"}
