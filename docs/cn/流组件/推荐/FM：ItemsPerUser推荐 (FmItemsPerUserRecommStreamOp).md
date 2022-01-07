# FM：ItemsPerUser推荐 (FmItemsPerUserRecommStreamOp)
Java 类名：com.alibaba.alink.operator.stream.recommendation.FmItemsPerUserRecommStreamOp

Python 类名：FmItemsPerUserRecommStreamOp


## 功能介绍
使用Fm推荐模型，为实时user推荐item list。


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

model = FmRecommTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setNumFactor(20)\
    .setRateCol("rating").linkFrom(data);

predictor = FmItemsPerUserRecommStreamOp(model)\
    .setUserCol("user")\
    .setK(1).setReservedCols(["user"])\
    .setRecommCol("prediction_result");

predictor.linkFrom(sdata).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.FmRecommTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.recommendation.FmItemsPerUserRecommStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FmItemsPerUserRecommStreamOpTest {
	@Test
	public void testFmItemsPerUserRecommStreamOp() throws Exception {
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
		BatchOperator <?> model = new FmRecommTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setNumFactor(20)
			.setRateCol("rating").linkFrom(data);
		StreamOperator <?> predictor = new FmItemsPerUserRecommStreamOp(model)
			.setUserCol("user")
			.setK(1).setReservedCols("user")
			.setRecommCol("prediction_result");
		predictor.linkFrom(sdata).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
user|	prediction_result
----|-----
1|	{"object":"[1]","rate":"[0.5829579830169678]"}
2|	{"object":"[2]","rate":"[0.576914370059967]"}
2|	{"object":"[2]","rate":"[0.576914370059967]"}
4|	{"object":"[1]","rate":"[0.5055253505706787]"}
4|	{"object":"[1]","rate":"[0.5055253505706787]"}
4|	{"object":"[1]","rate":"[0.5055253505706787]"}
