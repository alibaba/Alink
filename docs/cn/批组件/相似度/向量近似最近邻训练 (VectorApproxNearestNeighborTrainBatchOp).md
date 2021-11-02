# 向量近似最近邻训练 (VectorApproxNearestNeighborTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.similarity.VectorApproxNearestNeighborTrainBatchOp

Python 类名：VectorApproxNearestNeighborTrainBatchOp


## 功能介绍
该功能由训练和预测组成，训练时指定距离计算方式，生成最近邻模型

可选择的距离计算方式包含EUCLIDEAN和JACCARD两种，同时支持KDTREE和LSH两种近似方法。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| idCol | id列名 | id列名 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| numHashTables | 哈希表的数目 | 哈希表的数目 | Integer |  | 1 |
| numProjectionsPerTable | 每个哈希表中的哈希函数个数 | 每个哈希表中的哈希函数个数 | Integer |  | 1 |
| projectionWidth | 桶的宽度 | 桶的宽度 | Double |  | 1.0 |
| seed | 采样种子 | 采样种子 | Long |  | 0 |
| metric | 距离度量方式 | 距离类型 | String |  | "EUCLIDEAN" |
| solver | 近似方法 | 近似方法，包括KDTREE和LSH | String |  | "KDTREE" |


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
train = VectorApproxNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("vec").linkFrom(inOp)
predict = VectorApproxNearestNeighborPredictBatchOp().setSelectedCol("vec").setTopN(3).linkFrom(train, inOp)
predict.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.similarity.VectorApproxNearestNeighborPredictBatchOp;
import com.alibaba.alink.operator.batch.similarity.VectorApproxNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorApproxNearestNeighborTrainBatchOpTest {
	@Test
	public void testVectorApproxNearestNeighborTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "1 1 1"),
			Row.of(2, "2 2 2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, vec string");
		BatchOperator <?> train = new VectorApproxNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("vec")
			.linkFrom(inOp);
		BatchOperator <?> predict = new VectorApproxNearestNeighborPredictBatchOp().setSelectedCol("vec").setTopN(3)
			.linkFrom(train, inOp);
		predict.print();
	}
}
```

### 运行结果
id|vec
---|---
0|{"ID":"[0,1,2]","METRIC":"[0.0,1.7320508075688772,3.4641016151377544]"}
1|{"ID":"[1,2,0]","METRIC":"[0.0,1.7320508075688772,1.7320508075688772]"}
2|{"ID":"[2,1,0]","METRIC":"[0.0,1.7320508075688772,3.4641016151377544]"}

