# UserCf训练 (UserCfTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.UserCfTrainBatchOp

Python 类名：UserCfTrainBatchOp


## 功能介绍
UserCF 是一种被广泛使用的协同过滤算法，用给定打分数据训练一个推荐模型，
用于预测user对item的评分、对user推荐itemlist，或者对item推荐userlist等。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |  |
| k | 相似集合元素数目 | 相似集合元素数目 | Integer |  |  | 64 |
| rateCol | 打分列列名 | 打分列列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| similarityThreshold | 相似阈值 | 只有大于该阈值的Object才会被计算 | Double |  |  | 1.0E-4 |
| similarityType | 距离度量方式 | 聚类使用的距离类型 | String |  | "COSINE", "JACCARD", "PEARSON" | "COSINE" |

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

predictor = UserCfRateRecommBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.UserCfRateRecommBatchOp;
import com.alibaba.alink.operator.batch.recommendation.UserCfTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class UserCfTrainBatchOpTest {
	@Test
	public void testUserCfTrainBatchOp() throws Exception {
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
		BatchOperator <?> predictor = new UserCfRateRecommBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setRecommCol("prediction_result");
		predictor.linkFrom(model, data).print();
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
