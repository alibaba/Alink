# IForest模型异常检测预测 (IForestModelOutlierPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.outlier.IForestModelOutlierPredictStreamOp

Python 类名：IForestModelOutlierPredictStreamOp


## 功能介绍
iForest 可以识别数据中异常点，在异常检测领域有比较好的效果。算法使用 sub-sampling 方法，降低了算法的计算复杂度。

### 文献或出处
1. [Isolation Forest](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf?q=isolation-forest)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

## 代码示例

### Python 代码

```python
import pandas as pd
df = pd.DataFrame([
[0.73, 0],
[0.24, 0],
[0.63, 0],
[0.55, 0],
[0.73, 0],
[0.41, 0]
])

dataOp = BatchOperator.fromDataframe(df, schemaStr='val double, label int')

streamDataOp = StreamOperator.fromDataframe(df, schemaStr='val double, label int')

trainOp = IForestModelOutlierTrainBatchOp()\
.setFeatureCols(["val"])

predOp = IForestModelOutlierPredictStreamOp(trainOp.linkFrom(dataOp))\
.setOutlierThreshold(3.0)\
.setPredictionCol("pred")\
.setPredictionDetailCol("pred_detail")

predOp.linkFrom(streamDataOp).print()

StreamOperator.execute()
```

### Java 代码

```java
package com.alibaba.alink.operator.stream.outlier;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.outlier.IForestModelOutlierTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class IForestModelOutlierPredictStreamOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		Object[][] dataArrays = new Object[][] {
			{0.73, 0},
			{0.24, 0},
			{0.63, 0},
			{0.55, 0},
			{0.73, 0},
			{0.41, 0}
		};

		String[] colNames = new String[] {"val", "label"};

		BatchOperator <?> data = new MemSourceBatchOp(dataArrays, colNames);

		StreamOperator <?> streamData = new MemSourceStreamOp(dataArrays, colNames);

		IForestModelOutlierTrainBatchOp trainOp = new IForestModelOutlierTrainBatchOp()
			.setFeatureCols("val");

		IForestModelOutlierPredictStreamOp predOp = new IForestModelOutlierPredictStreamOp(trainOp.linkFrom(data))
			.setOutlierThreshold(3.0)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		predOp.linkFrom(streamData).print();

		StreamOperator.execute();

	}
}

```

### 运行结果

无
