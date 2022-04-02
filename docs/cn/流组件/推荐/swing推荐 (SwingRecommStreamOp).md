# swing推荐 (SwingRecommStreamOp)
Java 类名：com.alibaba.alink.operator.stream.recommendation.SwingRecommStreamOp

Python 类名：SwingRecommStreamOp


## 功能介绍
Swing 是一种被广泛使用的item召回算法，算法详细介绍可以参考SwingTrainBatchOp组件。

该组件为Swing的流处理预测组件，输入为 SwingTrainBatchOp 输出的模型和要预测的item列。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |
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

public class SwingRecommStreamOpTest {
	@Test
	public void testSwingRecommStreamOp() throws Exception {
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
a6|15L|2.0000|null
a4|13L|2.0000|null
a6|16L|2.0000|null
a5|14L|2.0000|null
a3|13L|2.0000|null
a1|12L|2.0000|{"item":"[\"11L\"]","score":"[0.11662912368774414]"}
a2|12L|2.0000|{"item":"[\"11L\"]","score":"[0.11662912368774414]"}
a1|11L|2.2000|{"item":"[\"12L\"]","score":"[0.12805642187595367]"}
a3|12L|2.0000|{"item":"[\"11L\"]","score":"[0.11662912368774414]"}
a4|14L|2.0000|null
a5|15L|2.0000|null
a2|11L|2.0000|{"item":"[\"12L\"]","score":"[0.12805642187595367]"}
