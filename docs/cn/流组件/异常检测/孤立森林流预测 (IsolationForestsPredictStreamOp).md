# 孤立森林流预测 (IsolationForestsPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.outlier.IsolationForestsPredictStreamOp

Python 类名：IsolationForestsPredictStreamOp


## 功能介绍
isolationforests是一种常用的树模型，在异常检测中常常可以取得很好的效果
## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
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

sourceFrame = pd.DataFrame([
        [1.0, 0],
        [2.0, 1],
        [3.0, 2],
        [4.0, 3]
    ])

batchSource = BatchOperator.fromDataframe(sourceFrame,schemaStr="f0 double, f1 double")
streamSource = StreamOperator.fromDataframe(sourceFrame,schemaStr="f0 double, f1 double")
trainOp = IsolationForestsTrainBatchOp().setFeatureCols(['f0', 'f1']).linkFrom(batchSource)

predictBatchOp = IsolationForestsPredictBatchOp().setPredictionCol('pred')

predictStreamOp = IsolationForestsPredictStreamOp(trainOp).setPredictionCol('pred')

predictStreamOp.linkFrom(streamSource).print()

StreamOperator.execute()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.outlier.IsolationForestsTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.outlier.IsolationForestsPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IsolationForestsPredictStreamOpTest {
	@Test
	public void testIsolationForestsPredictBatchOp() throws Exception {
		List <Row> sourceFrame = Arrays.asList(
			Row.of(1.0, 0.0),
			Row.of(2.0, 1.0),
			Row.of(3.0, 2.0),
			Row.of(4.0, 3.0)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(sourceFrame, "f0 double, f1 double");
		StreamOperator <?> streamSource = new MemSourceStreamOp(sourceFrame, "f0 double, f1 double");
		BatchOperator <?> trainOp =
			new IsolationForestsTrainBatchOp().setFeatureCols("f0", "f1").linkFrom(batchSource);

		streamSource
			.link(
				new IsolationForestsPredictStreamOp(trainOp)
					.setPredictionCol("pred")
			)
			.print();

		StreamOperator.execute();
	}
}
```

### 运行结果


f0|f1|pred
---|---|----
3.0000|2.0000|0.3817
1.0000|0.0000|0.3817
4.0000|3.0000|0.3817
2.0000|1.0000|0.3817
