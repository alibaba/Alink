# swing训练 (SwingTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.SwingTrainBatchOp

Python 类名：SwingTrainBatchOp


## 功能介绍
Swing 是一种被广泛使用的item召回算法

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| alpha | alpha参数 | alpha参数，默认1.0 | Float |  | 1.0 |
| rateCol | 打分列列名 | 打分列列名 | String |  | null |
| userAlpha | 用户alpha参数 | 用户alpha参数，默认5.0, user weight = 1.0/(userAlpha + userClickCount)^userBeta | Float |  | 5.0 |
| userBeta | 用户beta参数 | 用户beta参数，默认-0.35, user weight = 1.0/(userAlpha + userClickCount)^userBeta | Float |  | -0.35 |
| resultNormalize | 结果是否归一化 | 是否归一化，默认False | Boolean |  | false |
| maxItemNumber | item参与计算的人数最大值 | 如果item出现次数大于该次数，会随机选择该次数的用户数据，默认1000 | Integer |  | 1000 |
| minUserItems | 用户互动的最小Item数量 | 如果用户互动Item数量小于该次数，该用户数据不参与计算过程，默认10 | Integer |  | 10 |
| maxUserItems | 用户互动的最大Item数量 | 如果用户互动Item数量大于该次数，该用户数据不参与计算过程，默认1000 | Integer |  | 1000 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ["a1", "11L", 2.2],
    ["a1", "12L", 2.0],
    ["a2", "11L", 2.0],
    ["a2", "12L", 2.0],
    ["a3", "12L", 2.0],
    ["a3", "13L", 2.0],
    ["a4", "13L", 2.0],
    ["a4", "14L", 2.0],
    ["a5", "14L", 2.0],
    ["a5", "15L", 2.0],
    ["a6", "15L", 2.0],
    ["a6", "16L", 2.0],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='user string, item string, rating double')


model = SwingTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setMinUserItems(1)\
    .setRateCol("rating").linkFrom(data)

model.print()

predictor = SwingRecommBatchOp()\
    .setItemCol("item")\
    .setRecommCol("prediction_result")

predictor.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.SwingRecommBatchOp;
import com.alibaba.alink.operator.batch.recommendation.SwingTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SwingTrainBatchOpTest {
	@Test
	public void testSwingTrainBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("a1", "11L", 2.2),
			Row.of("a1", "12L", 2.0),
			Row.of("a2", "11L", 2.0),
			Row.of("a2", "12L", 2.0),
			Row.of("a3", "12L", 2.0),
			Row.of("a3", "13L", 2.0),
			Row.of("a4", "13L", 2.0),
			Row.of("a4", "14L", 2.0),
			Row.of("a5", "14L", 2.0),
			Row.of("a5", "15L", 2.0),
			Row.of("a6", "15L", 2.0),
			Row.of("a6", "16L", 2.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user string, item string, rating double");
		BatchOperator <?> model = new SwingTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
            .setMinUserItems(1)
			.setRateCol("rating").linkFrom(data);
		model.print();
		BatchOperator <?> predictor = new SwingRecommBatchOp()
			.setItemCol("item")
			.setRecommCol("prediction_result");
		predictor.linkFrom(model, data).print();
	}
}
```

### 运行结果
mainItems|recommItemAndSimilarity
---------|-----------------------
11L|{"object":["12L"],"score":[1.3015095],"itemCol":"item"}
14L|{"object":[],"score":[],"itemCol":"item"}
16L|{"object":[],"score":[],"itemCol":"item"}
15L|{"object":[],"score":[],"itemCol":"item"}
13L|{"object":[],"score":[],"itemCol":"item"}
12L|{"object":["11L"],"score":[1.3015095],"itemCol":"item"}

user|item|rating|prediction_result
----|----|------|-----------------
a1|11L|2.2000|{"item":"[\"12L\"]","score":"[1.3015094995498657]"}
a1|12L|2.0000|{"item":"[\"11L\"]","score":"[1.3015094995498657]"}
a2|11L|2.0000|{"item":"[\"12L\"]","score":"[1.3015094995498657]"}
a2|12L|2.0000|{"item":"[\"11L\"]","score":"[1.3015094995498657]"}
a3|12L|2.0000|{"item":"[\"11L\"]","score":"[1.3015094995498657]"}
a3|13L|2.0000|{"item":"[]","score":"[]"}
a4|13L|2.0000|{"item":"[]","score":"[]"}
a4|14L|2.0000|{"item":"[]","score":"[]"}
a5|14L|2.0000|{"item":"[]","score":"[]"}
a5|15L|2.0000|{"item":"[]","score":"[]"}
a6|15L|2.0000|{"item":"[]","score":"[]"}
a6|16L|2.0000|{"item":"[]","score":"[]"}
