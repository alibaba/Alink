# Prophet预测 (ProphetPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.timeseries.ProphetPredictStreamOp

Python 类名：ProphetPredictStreamOp


## 功能介绍
使用Prophet进行时间序列预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| valueCol | value列，类型为MTable | value列，类型为MTable | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；如果是目录，那么只能使用本地路径，即 file://。 | String |  | "" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| predictNum | 预测条数 | 预测条数 | Integer |  | 1 |
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

import time, datetime
import numpy as np
import pandas as pd

downloader = AlinkGlobalConfiguration.getPluginDownloader()
downloader.downloadPlugin('tf115_python_env_linux')

data = pd.DataFrame([
			[1,  datetime.datetime.fromtimestamp(1), 10.0],
			[1,  datetime.datetime.fromtimestamp(2), 11.0],
			[1,  datetime.datetime.fromtimestamp(3), 12.0],
			[1,  datetime.datetime.fromtimestamp(4), 13.0],
			[1,  datetime.datetime.fromtimestamp(5), 14.0],
			[1,  datetime.datetime.fromtimestamp(6), 15.0],
			[1,  datetime.datetime.fromtimestamp(7), 16.0],
			[1,  datetime.datetime.fromtimestamp(8), 17.0],
			[1,  datetime.datetime.fromtimestamp(9), 18.0],
			[1,  datetime.datetime.fromtimestamp(10), 19.0]
])

streamSource = dataframeToOperator(data, schemaStr='id int, ds1 timestamp, y1 double', op_type='stream')

over = OverCountWindowStreamOp()\
			.setTimeCol("ds1")\
			.setPrecedingRows(4)\
			.setClause("mtable_agg_preceding(ds1,y1) as tensor")

streamPred = ProphetStreamOp()\
			.setValueCol("tensor")\
			.setPredictNum(1)\
			.setPredictionCol("pred")\
			.setPredictionDetailCol("pred_detail")

valueOp = LookupVectorInTimeSeriesStreamOp()\
			.setTimeSeriesCol("pred")\
			.setTimeCol("ds1")\
			.setReservedCols(["ds1", "tensor", "pred"])\
			.setOutputCol("y_hat")

streamSource\
    .link(over)\
    .link(streamPred)\
    .link(valueOp)\
    .print()

StreamOperator.execute()
```
### Java 代码
```java
package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.timeseries.ProphetTrainBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;

public class ProphetPredictStreamOpTest {
	@Test
	public void testModel() throws Exception {
		Row[] rowsData =
			new Row[] {
				Row.of("1", new Timestamp(117, 11, 1, 0, 0, 0, 0), "9.59076113897809 9.59076113897809"),
				Row.of("1", new Timestamp(117, 11, 2, 0, 0, 0, 0), "8.51959031601596 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 3, 0, 0, 0, 0), "9.59076113897809 8.51959031601596"),
				Row.of("1", new Timestamp(117, 11, 4, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 5, 0, 0, 0, 0), "8.51959031601596 8.51959031601596"),
				Row.of("1", new Timestamp(117, 11, 6, 0, 0, 0, 0), "8.07246736935477 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 7, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 8, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 9, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 10, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 11, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 12, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
				Row.of("2", new Timestamp(117, 11, 13, 0, 0, 0, 0), "8.18367658262066 8.51959031601596"),
			};
		String[] colNames = new String[] {"id", "ds1", "y1"};

		//train batch model.
		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(rowsData), colNames);

		ProphetTrainBatchOp model = new ProphetTrainBatchOp()
			.setTimeCol("ds1")
			.setValueCol("y1");

		source.link(model);

		MemSourceStreamOp streamSource = new MemSourceStreamOp(Arrays.asList(rowsData), colNames);

		OverCountWindowStreamOp over = new OverCountWindowStreamOp()
			.setTimeCol("ds1")
			.setPrecedingRows(4)
			.setClause("mtable_agg_preceding(ds1,y1) as tensor");

		ProphetPredictStreamOp streamPred = new ProphetPredictStreamOp(model)
			.setValueCol("tensor")
			.setPredictNum(1)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		LookupVectorInTimeSeriesStreamOp valueOp = new LookupVectorInTimeSeriesStreamOp()
			.setTimeSeriesCol("pred")
			.setTimeCol("ds1")
			.setReservedCols("ds1", "tensor", "pred")
			.setOutputCol("y_hat");

		streamSource
			.link(over)
			.link(streamPred)
			.link(valueOp)
			.print();

		StreamOperator.execute();

	}
}
```

### 运行结果
id|data|predict
---|----|-------
1|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003","1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01"],"val":[10.0,11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016","1970-01-01 08:00:00.017","1970-01-01 08:00:00.018","1970-01-01 08:00:00.019","1970-01-01 08:00:00.02","1970-01-01 08:00:00.021","1970-01-01 08:00:00.022"],"val":[20.0,21.0,22.0,23.0,24.0,25.0,26.0,27.0,28.0,29.0,30.0,31.0]},"schema":"ts TIMESTAMP,val DOUBLE"}
t
