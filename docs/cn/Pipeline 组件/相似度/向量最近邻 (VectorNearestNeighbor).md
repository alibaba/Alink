# 向量最近邻 (VectorNearestNeighbor)
Java 类名：com.alibaba.alink.pipeline.similarity.VectorNearestNeighbor

Python 类名：VectorNearestNeighbor


## 功能介绍
该功能由训练和预测组成，支持计算1. 求最近邻topN 2. 求radius范围内的邻居。该功能由预测时候的topN和radius参数控制, 如果填写了topN，则输出最近邻，如果填写了radius，则输出radius范围内的邻居。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| idCol | id列名 | id列名 | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| metric | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN", "COSINE", "INNERPRODUCT", "CITYBLOCK", "JACCARD", "PEARSON" | "EUCLIDEAN" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| radius | radius值 | radius值 | Double |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | x >= 1 | null |
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
    [0, "0 0 0"],
    [1, "1 1 1"],
    [2, "2 2 2"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
pipeline = VectorNearestNeighbor().setIdCol("id").setSelectedCol("vec").setTopN(3)

pipeline.fit(inOp).transform(inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.similarity.VectorNearestNeighbor;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorNearestNeighborTest {
	@Test
	public void testVectorNearestNeighbor() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "1 1 1"),
			Row.of(2, "2 2 2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, vec string");
		VectorNearestNeighbor pipeline = new VectorNearestNeighbor().setIdCol("id").setSelectedCol("vec").setTopN(3);
		pipeline.fit(inOp).transform(inOp).print();
	}
}
```

### 运行结果
id|vec
---|---
0|{"ID":"[0,1,2]","METRIC":"[0.0,1.7320508075688772,3.4641016151377544]"}
1|{"ID":"[1,2,0]","METRIC":"[0.0,1.7320508075688772,1.7320508075688772]"}
2|{"ID":"[2,1,0]","METRIC":"[0.0,1.7320508075688772,3.4641016151377544]"}

