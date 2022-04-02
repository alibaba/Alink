# 向量最近邻训练 (VectorNearestNeighborTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborTrainBatchOp

Python 类名：VectorNearestNeighborTrainBatchOp


## 功能介绍
该组件为向量最近邻的训练过程，在计算时与 VectorNearestNeighborPredictBatchOp 配合使用。

支持的距离计算方式包含EUCLIDEAN，COSINE，INNERPRODUCT（内积），CITYBLOCK（曼哈顿距离），JACCARD，PEARSON

默认距离EUCLIDEAN

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| idCol | id列名 | id列名 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| metric | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN" |


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
train = VectorNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("vec").linkFrom(inOp)
predict = VectorNearestNeighborPredictBatchOp().setSelectedCol("vec").setTopN(3).linkFrom(train, inOp)
predict.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborPredictBatchOp;
import com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorNearestNeighborTrainBatchOpTest {
	@Test
	public void testVectorNearestNeighborTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "1 1 1"),
			Row.of(2, "2 2 2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, vec string");
		BatchOperator <?> train =
			new VectorNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("vec").linkFrom(
			inOp);
		BatchOperator <?> predict =
			new VectorNearestNeighborPredictBatchOp().setSelectedCol("vec").setTopN(3).linkFrom(
			train, inOp);
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

