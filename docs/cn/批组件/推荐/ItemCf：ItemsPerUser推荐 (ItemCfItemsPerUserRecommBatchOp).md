# ItemCf：ItemsPerUser推荐 (ItemCfItemsPerUserRecommBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.ItemCfItemsPerUserRecommBatchOp

Python 类名：ItemCfItemsPerUserRecommBatchOp


## 功能介绍
用ItemCF模型 为user推荐item list。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |  |
| excludeKnown | 排除已知的关联 | 推荐结果中是否排除训练数据中已知的关联 | Boolean |  |  | false |
| initRecommCol | 初始推荐列列名 | 初始推荐列列名 | String |  | 所选列类型为 [M_TABLE] | null |
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

model = ItemCfTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRateCol("rating").linkFrom(data);

predictor = ItemCfItemsPerUserRecommBatchOp()\
    .setUserCol("user")\
    .setReservedCols(["user"])\
    .setK(1)\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.ItemCfItemsPerUserRecommBatchOp;
import com.alibaba.alink.operator.batch.recommendation.ItemCfTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ItemCfItemsPerUserRecommBatchOpTest {
	@Test
	public void testItemCfItemsPerUserRecommBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1, 0.6),
			Row.of(2, 2, 0.8),
			Row.of(2, 3, 0.6),
			Row.of(4, 1, 0.6),
			Row.of(4, 2, 0.3),
			Row.of(4, 3, 0.4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int, rating double");
		BatchOperator <?> model = new ItemCfTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setRateCol("rating").linkFrom(data);
		BatchOperator <?> predictor = new ItemCfItemsPerUserRecommBatchOp()
			.setUserCol("user")
			.setReservedCols("user")
			.setK(1)
			.setRecommCol("prediction_result");
		predictor.linkFrom(model, data).print();
	}
}
```

### 运行结果
user|prediction_result
----|-----------------
1|{"item":"[3]","score":"[0.23533936216582085]"}
2|{"item":"[3]","score":"[0.38953648389671724]"}
2|{"item":"[3]","score":"[0.38953648389671724]"}
4|{"item":"[2]","score":"[0.17950184794838112]"}
4|{"item":"[2]","score":"[0.17950184794838112]"}
4|{"item":"[2]","score":"[0.17950184794838112]"}
