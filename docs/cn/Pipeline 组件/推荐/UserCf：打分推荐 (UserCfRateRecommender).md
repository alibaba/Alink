# UserCf：打分推荐 (UserCfRateRecommender)
Java 类名：com.alibaba.alink.pipeline.recommendation.UserCfRateRecommender

Python 类名：UserCfRateRecommender


## 功能介绍
UserCF 打分是使用UserCF模型，预测user对item的评分。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |
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

predictor = UserCfRateRecommender()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRecommCol("prediction_result")\
    .setModelData(model)

predictor.transform(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.UserCfTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.recommendation.UserCfRateRecommender;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class UserCfRateRecommenderTest {
	@Test
	public void testUserCfRateRecommender() throws Exception {
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
		UserCfRateRecommender predictor = new UserCfRateRecommender()
			.setUserCol("user")
			.setItemCol("item")
			.setRecommCol("prediction_result")
			.setModelData(model);
		predictor.transform(data).print();
	}
}
```

### 运行结果
user|item|rating|prediction_result
----|----|------|-----------------
1|1|0.6000|0.6000
2|2|0.8000|0.3000
2|3|0.6000|0.4000
4|1|0.6000|0.6000
4|2|0.3000|0.8000
4|3|0.4000|0.6000
