# 向量最近邻预测 (VectorNearestNeighborPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborPredictBatchOp

Python 类名：VectorNearestNeighborPredictBatchOp


## 功能介绍
该组件为向量最近邻预测功能，接收VectorNearestNeighborTrainBatchOp训练的模型

该功能由预测时候的topN和radius参数控制, 如果填写了topN，则输出最近邻，如果填写了radius，则输出radius范围内的邻居。

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

public class VectorNearestNeighborPredictBatchOpTest {
	@Test
	public void testVectorNearestNeighborPredictBatchOp() throws Exception {
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

