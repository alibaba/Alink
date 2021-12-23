# 字符串近似最近邻 (StringApproxNearestNeighbor)
Java 类名：com.alibaba.alink.pipeline.similarity.StringApproxNearestNeighbor

Python 类名：StringApproxNearestNeighbor


## 功能介绍

该功能由训练和预测组成，支持计算1. 求最近邻topN 2. 求radius范围内的邻居。该功能由预测时候的topN和radius参数控制, 如果填写了topN，则输出最近邻，如果填写了radius，则输出radius范围内的邻居。

SimhashHamming（SimHash_Hamming_Distance)相似度=1-距离/64.0，应选择metric的参数为SIMHASH_HAMMING_SIM。

MinHash应选择metric的参数为MINHASH_SIM。

Jaccard应选择metric的参数为JACCARD_SIM。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| idCol | id列名 | id列名 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| numBucket | 分桶个数 | 分桶个数 | Integer |  | 10 |
| numHashTables | 哈希表个数 | 哈希表的数目 | Integer |  | 10 |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| seed | 采样种子 | 采样种子 | Long |  | 0 |
| radius | radius值 | radius值 | Double |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | null |
| metric | 距离类型 | 用于计算的距离类型 | String |  | "SIMHASH_HAMMING_SIM" |
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

pipeline = StringApproxNearestNeighbor().setIdCol("id").setSelectedCol("text1").setMetric("SIMHASH_HAMMING_SIM").setTopN(3)

pipeline.fit(inOp).transform(inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.similarity.StringApproxNearestNeighbor;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StringApproxNearestNeighborTest {
	@Test
	public void testStringApproxNearestNeighbor() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "abcde", "aabce"),
			Row.of(1, "aacedw", "aabbed"),
			Row.of(2, "cdefa", "bbcefa"),
			Row.of(3, "bdefh", "ddeac"),
			Row.of(4, "acedm", "aeefbc")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, text1 string, text2 string");
		StringApproxNearestNeighbor pipeline = new StringApproxNearestNeighbor().setIdCol("id").setSelectedCol("text1")
			.setMetric("SIMHASH_HAMMING_SIM").setTopN(3);
		pipeline.fit(inOp).transform(inOp).print();
	}
}
```
### 运行结果
id|text1|text2
---|-----|-----
0|{"ID":"[0,1,4]","METRIC":"[1.0,0.96875,0.921875]"}|aabce
1|{"ID":"[1,0,4]","METRIC":"[1.0,0.96875,0.921875]"}|aabbed
2|{"ID":"[2,4,1]","METRIC":"[1.0,0.9375,0.890625]"}|bbcefa
3|{"ID":"[3,4,2]","METRIC":"[1.0,0.828125,0.828125]"}|ddeac
4|{"ID":"[4,2,1]","METRIC":"[1.0,0.9375,0.921875]"}|aeefbc
