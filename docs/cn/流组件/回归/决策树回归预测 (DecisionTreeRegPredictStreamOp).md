# 决策树回归预测 (DecisionTreeRegPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.regression.DecisionTreeRegPredictStreamOp

Python 类名：DecisionTreeRegPredictStreamOp


## 功能介绍

- 决策树回归组件支持稠密数据格式

- 支持带样本权重的训练

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
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

df = pd.DataFrame([
    [1.0, "A", 0, 0, 0],
    [2.0, "B", 1, 1, 0],
    [3.0, "C", 2, 2, 1],
    [4.0, "D", 3, 3, 1]
])
batchSource = BatchOperator.fromDataframe(
    df, schemaStr='f0 double, f1 string, f2 int, f3 int, label int')
streamSource = StreamOperator.fromDataframe(
    df, schemaStr='f0 double, f1 string, f2 int, f3 int, label int')

trainOp = DecisionTreeRegTrainBatchOp()\
    .setLabelCol('label')\
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
    .linkFrom(batchSource)
predictBatchOp = DecisionTreeRegPredictBatchOp()\
    .setPredictionCol('pred')
predictStreamOp = DecisionTreeRegPredictStreamOp(trainOp)\
    .setPredictionCol('pred')

predictBatchOp.linkFrom(trainOp, batchSource).print()
predictStreamOp.linkFrom(streamSource).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.DecisionTreeRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.DecisionTreeRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.DecisionTreeRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DecisionTreeRegPredictStreamOpTest {

	@Test
	public void testDecisionTreeRegPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.0, "A", 0L, 0L, 0L),
			Row.of(2.0, "B", 1L, 1L, 0L),
			Row.of(3.0, "C", 2L, 2L, 1L),
			Row.of(4.0, "D", 3L, 3L, 1L)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(
			df, "f0 double, f1 string, f2 long, f3 long, label long");
		StreamOperator <?> streamSource = new MemSourceStreamOp(
			df, "f0 double, f1 string, f2 long, f3 long, label long");
		BatchOperator <?> trainOp = new DecisionTreeRegTrainBatchOp()
			.setLabelCol("label")
			.setFeatureCols("f0", "f1", "f2", "f3")
			.linkFrom(batchSource);
		BatchOperator <?> predictBatchOp = new DecisionTreeRegPredictBatchOp()
			.setPredictionCol("pred");
		StreamOperator <?> predictStreamOp = new DecisionTreeRegPredictStreamOp(trainOp)
			.setPredictionCol("pred");
		predictBatchOp.linkFrom(trainOp, batchSource).print();
		predictStreamOp.linkFrom(streamSource).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

#### 批预测结果

f0|f1|f2|f3|label|pred
---|---|---|---|-----|----
1.0000|A|0|0|0|0.0000
2.0000|B|1|1|0|0.0000
3.0000|C|2|2|1|1.0000
4.0000|D|3|3|1|1.0000

#### 流预测结果

f0|f1|f2|f3|label|pred
---|---|---|---|-----|----
1.0000|A|0|0|0|0.0000
3.0000|C|2|2|1|1.0000
4.0000|D|3|3|1|1.0000
2.0000|B|1|1|0|0.0000

