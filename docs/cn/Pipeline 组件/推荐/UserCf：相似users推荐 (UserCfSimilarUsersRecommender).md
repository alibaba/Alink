# UserCf：相似users推荐 (UserCfSimilarUsersRecommender)
Java 类名：com.alibaba.alink.pipeline.recommendation.UserCfSimilarUsersRecommender

Python 类名：UserCfSimilarUsersRecommender


## 功能介绍
用UserCF模型为user推荐相似的user list。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| initRecommCol | 初始推荐列列名 | 初始推荐列列名 | String |  | null |
| k | 推荐TOP数量 | 推荐TOP数量 | Integer |  | 10 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |

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

predictor = UserCfSimilarUsersRecommender()\
    .setUserCol("user")\
    .setReservedCols(["user"])\
    .setK(1)\
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
import com.alibaba.alink.pipeline.recommendation.UserCfSimilarUsersRecommender;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class UserCfSimilarUsersRecommenderTest {
	@Test
	public void testUserCfSimilarUsersRecommender() throws Exception {
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
		UserCfSimilarUsersRecommender predictor = new UserCfSimilarUsersRecommender()
			.setUserCol("user")
			.setReservedCols("user")
			.setK(1)
			.setRecommCol("prediction_result")
			.setModelData(model);
		predictor.transform(data).print();
	}
}
```

### 运行结果
user|prediction_result
----|-----------------
1|{"user":"[4]","similarities":"[0.7682212795973759]"}
2|{"user":"[4]","similarities":"[0.6145770236779007]"}
2|{"user":"[4]","similarities":"[0.6145770236779007]"}
4|{"user":"[1]","similarities":"[0.7682212795973759]"}
4|{"user":"[1]","similarities":"[0.7682212795973759]"}
4|{"user":"[1]","similarities":"[0.7682212795973759]"}
