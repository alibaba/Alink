# UserCf：ItemsPerUser推荐 (UserCfItemsPerUserRecommStreamOp)
Java 类名：com.alibaba.alink.operator.stream.recommendation.UserCfItemsPerUserRecommStreamOp

Python 类名：UserCfItemsPerUserRecommStreamOp


## 功能介绍
用UserCF模型 实时为user推荐item list。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| excludeKnown | 排除已知的关联 | 推荐结果中是否排除训练数据中已知的关联 | Boolean |  | false |
| initRecommCol | 初始推荐列列名 | 初始推荐列列名 | String |  | null |
| k | 推荐TOP数量 | 推荐TOP数量 | Integer |  | 10 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

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
sdata = StreamOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')

model = UserCfTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRateCol("rating").linkFrom(data);

predictor = UserCfItemsPerUserRecommStreamOp(model)\
    .setUserCol("user")\
    .setReservedCols(["user"])\
    .setK(1)\
    .setRecommCol("prediction_result");

predictor.linkFrom(sdata).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.UserCfTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.recommendation.UserCfItemsPerUserRecommStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class UserCfItemsPerUserRecommStreamOpTest {
	@Test
	public void testUserCfItemsPerUserRecommStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1, 0.6),
			Row.of(2, 2, 0.8),
			Row.of(2, 3, 0.6),
			Row.of(4, 1, 0.6),
			Row.of(4, 2, 0.3),
			Row.of(4, 3, 0.4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int, rating double");
		StreamOperator <?> sdata = new MemSourceStreamOp(df_data, "user int, item int, rating double");
		BatchOperator <?> model = new UserCfTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setRateCol("rating").linkFrom(data);
		StreamOperator <?> predictor = new UserCfItemsPerUserRecommStreamOp(model)
			.setUserCol("user")
			.setReservedCols("user")
			.setK(1)
			.setRecommCol("prediction_result");
		predictor.linkFrom(sdata).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
user|prediction_result
----|-----------------
2|{"item":"[1]","score":"[0.1843731071033702]"}
4|{"item":"[2]","score":"[0.16388720631410686]"}
2|{"item":"[1]","score":"[0.1843731071033702]"}
4|{"item":"[2]","score":"[0.16388720631410686]"}
1|{"item":"[1]","score":"[0.4609327677584255]"}
4|{"item":"[2]","score":"[0.16388720631410686]"}
