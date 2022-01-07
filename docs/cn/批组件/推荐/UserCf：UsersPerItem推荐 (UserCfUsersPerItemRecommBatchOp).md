# UserCf：UsersPerItem推荐 (UserCfUsersPerItemRecommBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.UserCfUsersPerItemRecommBatchOp

Python 类名：UserCfUsersPerItemRecommBatchOp


## 功能介绍
用UserCF模型 为item推荐user list。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |
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

model = UserCfTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRateCol("rating").linkFrom(data);

predictor = UserCfUsersPerItemRecommBatchOp()\
    .setItemCol("item")\
    .setReservedCols(["item"])\
    .setK(1)\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.UserCfTrainBatchOp;
import com.alibaba.alink.operator.batch.recommendation.UserCfUsersPerItemRecommBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class UserCfUsersPerItemRecommBatchOpTest {
	@Test
	public void testUserCfUsersPerItemRecommBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1, 0.6),
			Row.of(2, 2, 0.8),
			Row.of(2, 3, 0.6),
			Row.of(4, 1, 0.6),
			Row.of(4, 2, 0.3),
			Row.of(4, 3, 0.4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int, rating double");
		BatchOperator <?> model = new UserCfTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setRateCol("rating").linkFrom(data);
		BatchOperator <?> predictor = new UserCfUsersPerItemRecommBatchOp()
			.setItemCol("item")
			.setReservedCols("item")
			.setK(1)
			.setRecommCol("prediction_result");
		predictor.linkFrom(model, data).print();
	}
}
```

### 运行结果
item|prediction_result
----|-----------------
1|{"user":"[1]","score":"[0.23046638387921276]"}
2|{"user":"[4]","score":"[0.2458308094711603]"}
3|{"user":"[4]","score":"[0.1843731071033702]"}
1|{"user":"[1]","score":"[0.23046638387921276]"}
2|{"user":"[4]","score":"[0.2458308094711603]"}
3|{"user":"[4]","score":"[0.1843731071033702]"}
