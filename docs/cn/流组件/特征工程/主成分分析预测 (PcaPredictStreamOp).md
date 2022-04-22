# 主成分分析预测 (PcaPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.feature.PcaPredictStreamOp

Python 类名：PcaPredictStreamOp


## 功能介绍

主成分分析，是考察多个变量间相关性一种多元统计方法，研究如何通过少数几个主成分来揭示多个变量间的内部结构，即从原始变量中导出少数几个主成分，使它们尽可能多地保留原始变量的信息，且彼此间互不相关，作为新的综合指标。详细介绍请见维基百科链接[wiki](https://en.wikipedia.org/wiki/Principal_component_analysis)。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["a", 1, 1, 2.0, True],
    ["c", 1, 2, -3.0, True],
    ["a", 2, 2, 2.0, False],
    ["c", 0, 0, 0.0, False]
    ])


batchSource = BatchOperator.fromDataframe(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')

streamSource = StreamOperator.fromDataframe(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')

trainOp = QuantileDiscretizerTrainBatchOp()\
    .setSelectedCols(['f_double'])\
    .setNumBuckets(8)\
    .linkFrom(batchSource)


predictBatchOp = QuantileDiscretizerPredictBatchOp()\
    .setSelectedCols(['f_double'])


predictBatchOp.linkFrom(trainOp,batchSource).print()

predictStreamOp = QuantileDiscretizerPredictStreamOp(trainOp)\
    .setSelectedCols(['f_double'])

predictStreamOp.linkFrom(streamSource).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.QuantileDiscretizerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PcaPredictStreamOpTest {
	@Test
	public void testPcaPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 1, 1, 2.0, true),
			Row.of("c", 1, 2, -3.0, true),
			Row.of("a", 2, 2, 2.0, false),
			Row.of("c", 0, 0, 0.0, false)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(df,
			"f_string string, f_long int, f_int int, f_double double, f_boolean boolean");
		StreamOperator <?> streamSource = new MemSourceStreamOp(df,
			"f_string string, f_long int, f_int int, f_double double, f_boolean boolean");
		BatchOperator <?> trainOp = new QuantileDiscretizerTrainBatchOp()
			.setSelectedCols("f_double")
			.setNumBuckets(8)
			.linkFrom(batchSource);
		BatchOperator <?> predictBatchOp = new QuantileDiscretizerPredictBatchOp()
			.setSelectedCols("f_double");
		predictBatchOp.linkFrom(trainOp, batchSource).print();
		StreamOperator <?> predictStreamOp = new QuantileDiscretizerPredictStreamOp(trainOp)
			.setSelectedCols("f_double");
		predictStreamOp.linkFrom(streamSource).print();
		StreamOperator.execute();
	}
}
```
### 运行结果

#### 批预测结果

f_string|f_long|f_int|f_double|f_boolean
 --------|------|-----|--------|---------
 a|1|1|2|true
 c|1|2|0|true
 a|2|2|2|false
 c|0|0|1|false
 
#### 流预测结果
 
 f_string|f_long|f_int|f_double|f_boolean
 --------|------|-----|--------|---------
 a|2|2|2|false
 c|1|2|0|true
 c|0|0|1|false
 a|1|1|2|true
