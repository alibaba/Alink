# 主成分分析预测 (PcaPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.PcaPredictBatchOp

Python 类名：PcaPredictBatchOp


## 功能介绍

主成分分析，是考察多个变量间相关性一种多元统计方法，研究如何通过少数几个主成分来揭示多个变量间的内部结构，即从原始变量中导出少数几个主成分，使它们尽可能多地保留原始变量的信息，且彼此间互不相关，作为新的综合指标。详细介绍请见维基百科链接[wiki](https://en.wikipedia.org/wiki/Principal_component_analysis)。

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
        [0.0,0.0,0.0],
        [0.1,0.2,0.1],
        [0.2,0.2,0.8],
        [9.0,9.5,9.7],
        [9.1,9.1,9.6],
        [9.2,9.3,9.9]
])

# batch source 
inOp = BatchOperator.fromDataframe(df, schemaStr='x1 double, x2 double, x3 double')

trainOp = PcaTrainBatchOp()\
       .setK(2)\
       .setSelectedCols(["x1","x2","x3"])

predictOp = PcaPredictBatchOp()\
        .setPredictionCol("pred")

# batch train
inOp.link(trainOp)

# batch predict
predictOp.linkFrom(trainOp,inOp)

predictOp.print()

# stream predict
inStreamOp = StreamOperator.fromDataframe(df, schemaStr='x1 double, x2 double, x3 double')

predictStreamOp = PcaPredictStreamOp(trainOp)\
        .setPredictionCol("pred")

predictStreamOp.linkFrom(inStreamOp)

predictStreamOp.print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.PcaPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.PcaTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.PcaPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PcaPredictBatchOpTest {
	@Test
	public void testPcaPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0.0, 0.0, 0.0),
			Row.of(0.1, 0.2, 0.1),
			Row.of(0.2, 0.2, 0.8),
			Row.of(9.0, 9.5, 9.7),
			Row.of(9.1, 9.1, 9.6),
			Row.of(9.2, 9.3, 9.9)
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "x1 double, x2 double, x3 double");
		BatchOperator <?> trainOp = new PcaTrainBatchOp()
			.setK(2)
			.setSelectedCols("x1", "x2", "x3");
		BatchOperator <?> predictOp = new PcaPredictBatchOp()
			.setPredictionCol("pred");
		inOp.link(trainOp);
		predictOp.linkFrom(trainOp, inOp);
		predictOp.print();
		StreamOperator <?> inStreamOp = new MemSourceStreamOp(df, "x1 double, x2 double, x3 double");
		StreamOperator <?> predictStreamOp = new PcaPredictStreamOp(trainOp)
			.setPredictionCol("pred");
		predictStreamOp.linkFrom(inStreamOp);
		predictStreamOp.print();
		StreamOperator.execute();
	}
}
```
### 运行结果

x1|x2|x3|pred
---|---|---|----
9.0|9.5|9.7|3.2280384305400736,1.1516225426477789E-4
0.2|0.2|0.8|0.13565076707329407,0.09003329494282108
9.2|9.3|9.9|3.250783163664603,0.0456526246528135
9.1|9.1|9.6|3.182618319978973,0.027469531992220464
0.1|0.2|0.1|0.045855205015063565,-0.012182917696915518
0.0|0.0|0.0|0.0,0.0


