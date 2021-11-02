# 线性回归预测 (LinearRegPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.regression.LinearRegPredictStreamOp

Python 类名：LinearRegPredictStreamOp


## 功能介绍
* 线性回归是一个回归算法
* 线性回归组件支持稀疏、稠密两种数据格式
* 线性回归组件支持带样本权重的训练

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
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
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])

batchData = BatchOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')
streamData = StreamOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')
colnames = ["f0","f1"]
lr = LinearRegTrainBatchOp()\
            .setFeatureCols(colnames)\
            .setLabelCol("label")

model = batchData.link(lr)

predictor = LinearRegPredictStreamOp(model)\
             .setPredictionCol("pred")

predictor.linkFrom(streamData).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.LinearRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LinearRegPredictStreamOpTest {
	@Test
	public void testLinearRegPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "f0 int, f1 int, label int");
		StreamOperator <?> streamData = new MemSourceStreamOp(df, "f0 int, f1 int, label int");
		String[] colnames = new String[] {"f0", "f1"};
		BatchOperator <?> lr = new LinearRegTrainBatchOp()
			.setFeatureCols(colnames)
			.setLabelCol("label");
		BatchOperator <?> model = batchData.link(lr);
		StreamOperator <?> predictor = new LinearRegPredictStreamOp(model)
			.setPredictionCol("pred");
		predictor.linkFrom(streamData).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
f0|f1|label|pred
---|---|-----|----
2|4|1|1.1765
2|1|1|1.0000
3|2|1|1.4118
4|3|2|1.8235
4|3|2|1.8235
1|2|1|0.7059
2|2|1|1.0588
