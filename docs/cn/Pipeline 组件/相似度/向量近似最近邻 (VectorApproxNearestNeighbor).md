# 向量近似最近邻 (VectorApproxNearestNeighbor)
Java 类名：com.alibaba.alink.pipeline.similarity.VectorApproxNearestNeighbor

Python 类名：VectorApproxNearestNeighbor


## 功能介绍
该功能由训练和预测组成，支持计算1. 求最近邻topN 2. 求radius范围内的邻居。该功能由预测时候的topN和radius参数控制, 如果填写了topN，则输出最近邻，如果填写了radius，则输出radius范围内的邻居。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| idCol | id列名 | id列名 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| numHashTables | 哈希表的数目 | 哈希表的数目 | Integer |  | 1 |
| numProjectionsPerTable | 每个哈希表中的哈希函数个数 | 每个哈希表中的哈希函数个数 | Integer |  | 1 |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| projectionWidth | 桶的宽度 | 桶的宽度 | Double |  | 1.0 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| seed | 采样种子 | 采样种子 | Long |  | 0 |
| radius | radius值 | radius值 | Double |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | null |
| metric | 距离度量方式 | 距离类型 | String |  | "EUCLIDEAN" |
| solver | 近似方法 | 近似方法，包括KDTREE和LSH | String |  | "KDTREE" |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


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
pipeline = VectorApproxNearestNeighbor().setIdCol("id").setSelectedCol("vec").setTopN(3)
pipeline.fit(inOp).transform(inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.similarity.VectorApproxNearestNeighbor;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorApproxNearestNeighborTest {
	@Test
	public void testVectorApproxNearestNeighbor() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "1 1 1"),
			Row.of(2, "2 2 2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, vec string");
		VectorApproxNearestNeighbor pipeline = new VectorApproxNearestNeighbor().setIdCol("id").setSelectedCol("vec")
			.setTopN(3);
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


