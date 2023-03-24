# ALS：相似users推荐 (AlsSimilarUsersRecommBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.AlsSimilarUsersRecommBatchOp

Python 类名：AlsSimilarUsersRecommBatchOp


## 功能介绍
使用ALS (Alternating Lease Square）model 对相似的user的进行推荐。这里的ALS模型可以是隐式模型，也可以是显式模型，输出格式是MTable。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |  |
| initRecommCol | 初始推荐列列名 | 初始推荐列列名 | String |  | 所选列类型为 [M_TABLE, STRING] | null |
| k | 推荐TOP数量 | 推荐TOP数量 | Integer |  |  | 10 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    [1, 1, 0.6],
    [2, 2, 0.8],
    [2, 3, 0.6],
    [4, 1, 0.6],
    [4, 2, 0.3],
    [4, 3, 0.4],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')

als = AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
    .setNumIter(10).setRank(10).setLambda(0.01)

model = als.linkFrom(data)
predictor = AlsSimilarUsersRecommBatchOp() \
    .setUserCol("user").setRecommCol("rec").setK(1).setReservedCols(["user"])

predictor.linkFrom(model, data).print();

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.AlsSimilarUsersRecommBatchOp;
import com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AlsSimilarUsersRecommBatchOpTest {
	@Test
	public void testAlsSimilarUsersRecommBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1, 0.6),
			Row.of(2, 2, 0.8),
			Row.of(2, 3, 0.6),
			Row.of(4, 1, 0.6),
			Row.of(4, 2, 0.3),
			Row.of(4, 3, 0.4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int, rating double");
		BatchOperator <?> als = new AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating")
			.setNumIter(10).setRank(10).setLambda(0.01);
		BatchOperator model = als.linkFrom(data);
		BatchOperator <?> predictor = new AlsSimilarUsersRecommBatchOp()
			.setUserCol("user").setRecommCol("rec").setK(1).setReservedCols("user");
		predictor.linkFrom(model, data).print();
	}
}
```

### 运行结果

user| rec
----|-------
1	|{"object":"[4]","score":"[0.2515771985054016]"}
2	|{"object":"[1]","score":"[0.17212671041488647]"}
2	|{"object":"[1]","score":"[0.17212671041488647]"}
4	|{"object":"[1]","score":"[0.2515771985054016]"}
4	|{"object":"[1]","score":"[0.2515771985054016]"}
4	|{"object":"[1]","score":"[0.2515771985054016]"}
