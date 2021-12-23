# Cross特征预测 (CrossFeaturePredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.feature.CrossFeaturePredictStreamOp

Python 类名：CrossFeaturePredictStreamOp


## 功能介绍
特征列组合算法能够将选定的离散列组合成单列的向量类型的数据。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
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
["1.0", "1.0", 1.0, 1],
["1.0", "1.0", 0.0, 1],
["1.0", "0.0", 1.0, 1],
["1.0", "0.0", 1.0, 1],
["2.0", "3.0", None, 0],
["2.0", "3.0", 1.0, 0],
["0.0", "1.0", 2.0, 0],
["0.0", "1.0", 1.0, 0]])
batchData = BatchOperator.fromDataframe(df, schemaStr="f0 string, f1 string, f2 double, label bigint")
streamData = StreamOperator.fromDataframe(df, schemaStr="f0 string, f1 string, f2 double, label bigint")
train = CrossFeatureTrainBatchOp().setSelectedCols(['f0','f1','f2']).linkFrom(batchData)
CrossFeaturePredictStreamOp(train).setOutputCol("cross").linkFrom(streamData).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.CrossFeaturePredictBatchOp;
import com.alibaba.alink.operator.batch.feature.CrossFeatureTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CrossFeaturePredictStreamOpTest {
	@Test
	public void testCrossFeaturePredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1.0", "1.0", 1.0, 1),
			Row.of("1.0", "1.0", 0.0, 1),
			Row.of("1.0", "0.0", 1.0, 1),
			Row.of("1.0", "0.0", 1.0, 1),
			Row.of("2.0", "3.0", null, 0),
			Row.of("2.0", "3.0", 1.0, 0),
			Row.of("0.0", "1.0", 2.0, 0)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "f0 string, f1 string, f2 double, label int");
		StreamOperator<?> streamData = new MemSourceStreamOp(df, "f0 string, f1 string, f2 double, label int");
		BatchOperator <?> train = new CrossFeatureTrainBatchOp().setSelectedCols("f0", "f1", "f2").linkFrom(batchData);
		new CrossFeaturePredictStreamOp(train).setOutputCol("cross").linkFrom(streamData).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

f0|f1|f2|label|cross
---|---|---|-----|-----
2.0|3.0|1.0000|0|$36$7:1.0
0.0|1.0|2.0000|0|$36$32:1.0
1.0|1.0|0.0000|1|$36$12:1.0
1.0|1.0|1.0000|1|$36$3:1.0
1.0|0.0|1.0000|1|$36$0:1.0
1.0|0.0|1.0000|1|$36$0:1.0
2.0|3.0|null|0|$36$25:1.0
