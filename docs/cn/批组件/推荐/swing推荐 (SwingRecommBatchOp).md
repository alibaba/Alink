# swing推荐 (SwingRecommBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.SwingRecommBatchOp

Python 类名：SwingRecommBatchOp


## 功能介绍
Swing 是一种被广泛使用的item召回算法，算法详细介绍可以参考SwingTrainBatchOp组件。

该组件为Swing的批处理预测组件，输入为 SwingTrainBatchOp 输出的模型和要预测的item列。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |  |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |  |
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
    .setMinUserItems(2)\
    .linkFrom(data)

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

public class SwingRecommBatchOpTest {
	@Test
	public void testSwingRecommBatchOp() throws Exception {
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
			.setMinUserItems(2)
			.linkFrom(data);
		BatchOperator <?> predictor = new SwingRecommBatchOp()
			.setItemCol("item")
			.setRecommCol("prediction_result");
		predictor.linkFrom(model, data).print();
	}
}
```

### 运行结果
user|item|rating|prediction_result
----|----|------|-----------------
a1|11L|2.2000|{"item":"[\"12L\"]","score":"[0.12805642187595367]"}
a1|12L|2.0000|{"item":"[\"11L\"]","score":"[0.11662912368774414]"}
a2|11L|2.0000|{"item":"[\"12L\"]","score":"[0.12805642187595367]"}
a2|12L|2.0000|{"item":"[\"11L\"]","score":"[0.11662912368774414]"}
a3|12L|2.0000|{"item":"[\"11L\"]","score":"[0.11662912368774414]"}
a3|13L|2.0000|null
a4|13L|2.0000|null
a4|14L|2.0000|null
a5|14L|2.0000|null
a5|15L|2.0000|null
a6|15L|2.0000|null
a6|16L|2.0000|null
