# LSTNet预测 (LSTNetPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.timeseries.LSTNetPredictStreamOp

Python 类名：LSTNetPredictStreamOp


## 功能介绍
使用 LSTNet 进行时间序列训练和预测。

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
    [0, datetime.datetime.fromisoformat("2021-11-01 00:00:00"), 100.0],
    [0, datetime.datetime.fromisoformat("2021-11-02 00:00:00"), 200.0],
    [0, datetime.datetime.fromisoformat("2021-11-03 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-11-04 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-11-06 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-11-07 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-08 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-09 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-10 00:00:00"), 900.0],
    [0, datetime.datetime.fromisoformat("2021-11-11 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-12 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-13 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-14 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-11-15 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-11-16 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-11-17 00:00:00"), 200.0],
    [0, datetime.datetime.fromisoformat("2021-11-18 00:00:00"), 100.0],
    [0, datetime.datetime.fromisoformat("2021-11-19 00:00:00"), 200.0],
    [0, datetime.datetime.fromisoformat("2021-11-20 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-11-21 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-11-22 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-11-23 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-24 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-25 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-26 00:00:00"), 900.0],
    [0, datetime.datetime.fromisoformat("2021-11-27 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-28 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-29 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-30 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-12-01 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-12-02 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-12-03 00:00:00"), 200.0]
])

batch_source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='batch')

stream_source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='stream')

lstNetTrainBatchOp = LSTNetTrainBatchOp()\
    .setTimeCol("ts")\
    .setSelectedCol("series")\
    .setNumEpochs(10)\
    .setWindow(24)\
    .setHorizon(1)\
    .linkFrom(batch_source)

overCountWindowStreamOp = OverCountWindowStreamOp()\
    .setClause("MTABLE_AGG_PRECEDING(ts, series) as mtable_agg_series")\
    .setTimeCol("ts")\
    .setPrecedingRows(24)

lstNetPredictStreamOp = LSTNetPredictStreamOp(lstNetTrainBatchOp)\
    .setPredictNum(1)\
    .setPredictionCol("pred")\
    .setReservedCols([])\
    .setValueCol("mtable_agg_series")

lstNetPredictStreamOp\
    .linkFrom(
        overCountWindowStreamOp\
        .linkFrom(stream_source)\
        .filter("ts = TO_TIMESTAMP('2021-12-03 00:00:00')")
    )\
    .print();

StreamOperator.execute();
```
### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.timeseries.LSTNetPredictStreamOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class LSTNetPredictStreamOpTest {

	@Test
	public void testLSTNetTrainBatchOp() throws Exception {
		BatchOperator.setParallelism(1);

		List <Row> data = Arrays.asList(
			Row.of(0, Timestamp.valueOf("2021-11-01 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-02 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-03 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-04 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-06 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-07 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-08 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-09 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-10 00:00:00"), 900.0),
			Row.of(0, Timestamp.valueOf("2021-11-11 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-12 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-13 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-14 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-15 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-16 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-17 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-18 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-19 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-20 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-21 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-22 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-23 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-24 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-25 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-26 00:00:00"), 900.0),
			Row.of(0, Timestamp.valueOf("2021-11-27 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-28 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-29 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-30 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-12-01 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-12-02 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-12-03 00:00:00"), 200.0)
		);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			data, "id int, ts timestamp, series double"
		);

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(
			data, "id int, ts timestamp, series double"
		);

		LSTNetTrainBatchOp lstNetTrainBatchOp = new LSTNetTrainBatchOp()
			.setTimeCol("ts")
			.setSelectedCol("series")
			.setNumEpochs(10)
			.setWindow(24)
			.setHorizon(1)
			.linkFrom(memSourceBatchOp);

		OverCountWindowStreamOp overCountWindowStreamOp = new OverCountWindowStreamOp()
			.setClause("MTABLE_AGG_PRECEDING(ts, series) as mtable_agg_series")
			.setTimeCol("ts")
			.setPrecedingRows(24);

		LSTNetPredictStreamOp lstNetPredictStreamOp = new LSTNetPredictStreamOp(lstNetTrainBatchOp)
			.setPredictNum(1)
			.setPredictionCol("pred")
			.setReservedCols()
			.setValueCol("mtable_agg_series");

		lstNetPredictStreamOp
			.linkFrom(
				overCountWindowStreamOp
					.linkFrom(memSourceStreamOp)
					.filter("ts = TO_TIMESTAMP('2021-12-03 00:00:00')")
			)
			.print();

		StreamOperator.execute();
	}

}
```

### 运行结果
| pred                                                                                                          |
|---------------------------------------------------------------------------------------------------------------|
| {"data":{"ts":["2021-12-04 00:00:00.0"],"series":[441.76019287109375]},"schema":"ts TIMESTAMP,series DOUBLE"} |
