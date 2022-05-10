# DeepAR预测 (DeepARPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.timeseries.DeepARPredictStreamOp

Python 类名：DeepARPredictStreamOp


## 功能介绍
使用 DeepAR 进行时间序列训练和预测。

### 使用方式

参考文档 https://www.yuque.com/pinshu/alink_guide/xbp5ky

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| valueCol | value列，类型为MTable | value列，类型为MTable | String | ✓ | 所选列类型为 [M_TABLE] |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| predictNum | 预测条数 | 预测条数 | Integer |  |  | 1 |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
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

import time, datetime
import numpy as np
import pandas as pd

data = pd.DataFrame([
    [0,  datetime.datetime.fromisoformat('2021-11-01 00:00:00'), 100.0],
    [0,  datetime.datetime.fromisoformat('2021-11-02 00:00:00'), 100.0],
    [0,  datetime.datetime.fromisoformat('2021-11-03 00:00:00'), 100.0],
    [0,  datetime.datetime.fromisoformat('2021-11-04 00:00:00'), 100.0],
    [0,  datetime.datetime.fromisoformat('2021-11-05 00:00:00'), 100.0]
])

batch_source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='batch')
stream_source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='stream')

deepARTrainBatchOp = DeepARTrainBatchOp()\
    .setTimeCol("ts")\
    .setSelectedCol("series")\
    .setNumEpochs(10)\
    .setWindow(2)\
    .setStride(1)\
    .linkFrom(batch_source)

overCountWindowStreamOp = OverCountWindowStreamOp()\
    .setClause("MTABLE_AGG_PRECEDING(ts, series) as mtable_agg_series")\
    .setTimeCol("ts")\
    .setPrecedingRows(2)

deepARPredictStreamOp = DeepARPredictStreamOp(deepARTrainBatchOp)\
    .setPredictNum(2)\
    .setPredictionCol("pred")\
    .setValueCol("mtable_agg_series")

deepARPredictStreamOp\
    .linkFrom(
        overCountWindowStreamOp\
        .linkFrom(stream_source)\
        .filter("ts = TO_TIMESTAMP('2021-11-05 00:00:00')")
    )\
    .print()

StreamOperator.execute()
```
### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.timeseries.DeepARPredictStreamOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class DeepARPredictStreamOpTest {

	@Test
	public void testDeepARTrainBatchOp() throws Exception {
		BatchOperator.setParallelism(1);

		List <Row> data = Arrays.asList(
			Row.of(0, Timestamp.valueOf("2021-11-01 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-02 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-03 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-04 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-05 00:00:00"), 100.0)
		);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "id int, ts timestamp, series double");

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(data, "id int, ts timestamp, series double");

		DeepARTrainBatchOp deepARTrainBatchOp = new DeepARTrainBatchOp()
			.setTimeCol("ts")
			.setSelectedCol("series")
			.setNumEpochs(10)
			.setWindow(2)
			.setStride(1)
			.linkFrom(memSourceBatchOp);

		OverCountWindowStreamOp overCountWindowStreamOp = new OverCountWindowStreamOp()
			.setClause("MTABLE_AGG_PRECEDING(ts, series) as mtable_agg_series")
			.setTimeCol("ts")
			.setPrecedingRows(2);

		DeepARPredictStreamOp deepARPredictStreamOp = new DeepARPredictStreamOp(deepARTrainBatchOp)
			.setPredictNum(2)
			.setPredictionCol("pred")
			.setValueCol("mtable_agg_series");

		deepARPredictStreamOp
			.linkFrom(
				overCountWindowStreamOp
					.linkFrom(memSourceStreamOp)
					.filter("ts = TO_TIMESTAMP('2021-11-05 00:00:00')")
			)
			.print();

		StreamOperator.execute();
	}

}
```

### 运行结果
| id | mtable_agg_series                                                                                                                                                                                                        | pred                                                                                                                                                    |
|----+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | {"data":{"ts":["2021-11-01 00:00:00.0","2021-11-02 00:00:00.0","2021-11-03 00:00:00.0","2021-11-04 00:00:00.0","2021-11-05 00:00:00.0"],"series":[100.0,100.0,100.0,100.0,100.0]},"schema":"ts TIMESTAMP,series DOUBLE"} | {"data":{"ts":["2021-11-06 00:00:00.0","2021-11-07 00:00:00.0"],"series":[31.424224853515625,39.10265350341797]},"schema":"ts TIMESTAMP,series DOUBLE"} |
