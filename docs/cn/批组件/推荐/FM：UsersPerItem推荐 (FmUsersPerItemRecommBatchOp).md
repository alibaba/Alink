# FM：UsersPerItem推荐 (FmUsersPerItemRecommBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.FmUsersPerItemRecommBatchOp

Python 类名：FmUsersPerItemRecommBatchOp


## 功能介绍
使用Fm推荐模型，为item推荐user list。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |  |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |  |
| excludeKnown | 排除已知的关联 | 推荐结果中是否排除训练数据中已知的关联 | Boolean |  |  | false |
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

model = FmRecommTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setNumFactor(20)\
    .setRateCol("rating").linkFrom(data);

predictor = FmRateRecommBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()

model = FmRecommTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setNumFactor(20)\
    .setRateCol("rating").linkFrom(data);

predictor = FmUsersPerItemRecommBatchOp()\
    .setItemCol("user")\
    .setK(1).setReservedCols(["item"])\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.FmRateRecommBatchOp;
import com.alibaba.alink.operator.batch.recommendation.FmRecommTrainBatchOp;
import com.alibaba.alink.operator.batch.recommendation.FmUsersPerItemRecommBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FmUsersPerItemRecommBatchOpTest {
	@Test
	public void testFmUsersPerItemRecommBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1, 0.6),
			Row.of(2, 2, 0.8),
			Row.of(2, 3, 0.6),
			Row.of(4, 1, 0.6),
			Row.of(4, 2, 0.3),
			Row.of(4, 3, 0.4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int, rating double");
		BatchOperator <?> model = new FmRecommTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setNumFactor(20)
			.setRateCol("rating").linkFrom(data);
		BatchOperator <?> predictor = new FmRateRecommBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setRecommCol("prediction_result");
		predictor.linkFrom(model, data).print();
		model = new FmRecommTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setNumFactor(20)
			.setRateCol("rating").linkFrom(data);
		predictor = new FmUsersPerItemRecommBatchOp()
			.setItemCol("user")
			.setK(1).setReservedCols("item")
			.setRecommCol("prediction_result");
		predictor.linkFrom(model, data).print();
	}
}
```

### 运行结果
item|	prediction_result
----|-----
1|	{"object":"[1]","rate":"[0.5829579830169678]"}
2|	{"object":"[2]","rate":"[0.576914370059967]"}
3|	{"object":"[1]","rate":"[0.5055253505706787]"}
1|	{"object":"[1]","rate":"[0.5829579830169678]"}
2|	{"object":"[2]","rate":"[0.576914370059967]"}
3|	{"object":"[1]","rate":"[0.5055253505706787]"}
