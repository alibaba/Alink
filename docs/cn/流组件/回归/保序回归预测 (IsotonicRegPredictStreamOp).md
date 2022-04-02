# 保序回归预测 (IsotonicRegPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.regression.IsotonicRegPredictStreamOp

Python 类名：IsotonicRegPredictStreamOp


## 功能介绍

保序回归在观念上是寻找一组非递减的片段连续线性函数（piecewise linear continuous functions），即保序函数，使其与样本尽可能的接近。
## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
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

df = pd.DataFrame([
    [0.35, 1],
    [0.6, 1],
    [0.55, 1],
    [0.5, 1],
    [0.18, 0],
    [0.1, 1],
    [0.8, 1],
    [0.45, 0],
    [0.4, 1],
    [0.7, 0],
    [0.02, 1],
    [0.3, 0],
    [0.27, 1],
    [0.2, 0],
    [0.9, 1]])

data = BatchOperator.fromDataframe(df, schemaStr="label double, feature double")
dataStream = StreamOperator.fromDataframe(df, schemaStr="label double, feature double")

trainOp = IsotonicRegTrainBatchOp()\
            .setFeatureCol("feature")\
            .setLabelCol("label")

model = trainOp.linkFrom(data)

predictOp = IsotonicRegPredictStreamOp(model)\
        .setPredictionCol("result")

predictOp.linkFrom(dataStream).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.IsotonicRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.IsotonicRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IsotonicRegPredictStreamOpTest {
	@Test
	public void testIsotonicRegPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0.35, 1.0),
			Row.of(0.6, 1.0),
			Row.of(0.55, 1.0),
			Row.of(0.5, 1.0),
			Row.of(0.18, 0.0),
			Row.of(0.1, 1.0),
			Row.of(0.8, 1.0),
			Row.of(0.45, 0.0),
			Row.of(0.4, 1.0),
			Row.of(0.7, 0.0),
			Row.of(0.02, 1.0),
			Row.of(0.3, 0.0),
			Row.of(0.27, 1.0),
			Row.of(0.2, 0.0),
			Row.of(0.9, 1.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "feature double, label double");
		StreamOperator <?> dataStream = new MemSourceStreamOp(df, "feature double, label double");
		BatchOperator <?> trainOp = new IsotonicRegTrainBatchOp()
			.setFeatureCol("feature")
			.setLabelCol("label");
		BatchOperator <?> model = trainOp.linkFrom(data);
		StreamOperator <?> predictOp = new IsotonicRegPredictStreamOp(model)
			.setPredictionCol("result");
		predictOp.linkFrom(dataStream).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
#### 模型结果
| model_id   | model_info |
| --- | --- |
| 0          | {"vectorCol":"\"col2\"","featureIndex":"0","featureCol":null} |
| 1048576    | [0.02,0.3,0.35,0.45,0.5,0.7] |
| 2097152    | [0.5,0.5,0.6666666865348816,0.6666666865348816,0.75,0.75] |
#### 预测结果
| col1       | col2       | col3       | pred       |
| --- | --- | --- | --- |
| 1.0        | 0.9        | 1.0        | 0.75       |
| 0.0        | 0.7        | 1.0        | 0.75       |
| 1.0        | 0.35       | 1.0        | 0.6666666865348816 |
| 1.0        | 0.02       | 1.0        | 0.5        |
| 1.0        | 0.27       | 1.0        | 0.5        |
| 1.0        | 0.5        | 1.0        | 0.75       |
| 0.0        | 0.18       | 1.0        | 0.5        |
| 0.0        | 0.45       | 1.0        | 0.6666666865348816 |
| 1.0        | 0.8        | 1.0        | 0.75       |
| 1.0        | 0.6        | 1.0        | 0.75       |
| 1.0        | 0.4        | 1.0        | 0.6666666865348816 |
| 0.0        | 0.3        | 1.0        | 0.5        |
| 1.0        | 0.55       | 1.0        | 0.75       |
| 0.0        | 0.2        | 1.0        | 0.5        |
| 1.0        | 0.1        | 1.0        | 0.5        |
