# ItemCf训练 (ItemCfTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.ItemCfTrainBatchOp

Python 类名：ItemCfTrainBatchOp


## 功能介绍
ItemCF 是一种被广泛使用的推荐算法，用给定打分数据训练一个推荐模型，
用于预测user对item的评分、对user推荐itemlist，或者对item推荐userlist。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| rateCol | 打分列列名 | 打分列列名 | String |  | null |
| similarityThreshold | Not available! | Not available! | Double |  | 1.0E-4 |
| similarityType | 距离度量方式 | 聚类使用的距离类型 | String |  | "COSINE" |
| maxNeighborNumber | Not available! | Not available! | Integer |  | 64 |

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

predictor = ItemCfRateRecommBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.ItemCfRateRecommBatchOp;
import com.alibaba.alink.operator.batch.recommendation.ItemCfTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ItemCfTrainBatchOpTest {
	@Test
	public void testItemCfTrainBatchOp() throws Exception {
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
		BatchOperator <?> predictor = new ItemCfRateRecommBatchOp()
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
1|1|0.6000|0.0000
2|2|0.8000|0.6000
2|3|0.6000|0.8000
4|1|0.6000|0.3612
4|2|0.3000|0.4406
4|3|0.4000|0.3861
