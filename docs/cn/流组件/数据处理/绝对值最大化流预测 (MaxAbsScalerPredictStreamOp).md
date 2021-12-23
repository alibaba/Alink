# 绝对值最大化流预测 (MaxAbsScalerPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.MaxAbsScalerPredictStreamOp

Python 类名：MaxAbsScalerPredictStreamOp


## 功能介绍

- 绝对值最大标准化是对数据按照最大值和最小值进行标准化的组件, 将数据归一到-1和1之间。
- 需要读入MaxAbsScalerTrainBatchOp生成的模型

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
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
            ["a", 10.0, 100],
            ["b", -2.5, 9],
            ["c", 100.2, 1],
            ["d", -99.9, 100],
            ["a", 1.4, 1],
            ["b", -2.2, 9],
            ["c", 100.9, 1]
])
             
colnames = ["col1", "col2", "col3"]
selectedColNames = ["col2", "col3"]

inOp = BatchOperator.fromDataframe(df, schemaStr='col1 string, col2 double, col3 long')
         
# train
trainOp = MaxAbsScalerTrainBatchOp()\
           .setSelectedCols(selectedColNames)

trainOp.linkFrom(inOp)

# batch predict
predictOp = MaxAbsScalerPredictBatchOp()
predictOp.linkFrom(trainOp, inOp).print()

# stream predict
sinOp = StreamOperator.fromDataframe(df, schemaStr='col1 string, col2 double, col3 long')

predictStreamOp = MaxAbsScalerPredictStreamOp(trainOp)
predictStreamOp.linkFrom(sinOp).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.MaxAbsScalerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MaxAbsScalerPredictStreamOpTest {
	@Test
	public void testMaxAbsScalerPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 10.0, 100),
			Row.of("b", -2.5, 9),
			Row.of("c", 100.2, 1),
			Row.of("d", -99.9, 100),
			Row.of("a", 1.4, 1),
			Row.of("b", -2.2, 9),
			Row.of("c", 100.9, 1)
		);

		String[] selectedColNames = new String[] {"col2", "col3"};
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "col1 string, col2 double, col3 int");
		BatchOperator <?> trainOp = new MaxAbsScalerTrainBatchOp()
			.setSelectedCols(selectedColNames);
		trainOp.linkFrom(inOp);
		BatchOperator <?> predictOp = new MaxAbsScalerPredictBatchOp();
		predictOp.linkFrom(trainOp, inOp).print();
		StreamOperator <?> sinOp = new MemSourceStreamOp(df, "col1 string, col2 double, col3 int");
		StreamOperator <?> predictStreamOp = new MaxAbsScalerPredictStreamOp(trainOp);
		predictStreamOp.linkFrom(sinOp).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

col1|col2|col3
----|----|----
a|0.0991|1.0000
b|-0.0248|0.0900
c|0.9931|0.0100
d|-0.9901|1.0000
a|0.0139|0.0100
b|-0.0218|0.0900
c|1.0000|0.0100

col1|col2|col3
----|----|----
b|-0.0248|0.0900
d|-0.9901|1.0000
a|0.0139|0.0100
c|0.9931|0.0100
c|1.0000|0.0100
a|0.0991|1.0000
b|-0.0218|0.0900



