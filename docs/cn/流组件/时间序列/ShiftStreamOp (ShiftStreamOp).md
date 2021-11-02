# ShiftStreamOp (ShiftStreamOp)
Java 类名：com.alibaba.alink.operator.stream.timeseries.ShiftStreamOp

Python 类名：ShiftStreamOp


## 功能介绍
使用Shift进行时间序列预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| valueCol | Not available! | Not available! | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| shiftNum | shift个数 | shift个数 | Integer |  | 7 |
| predictNum | Not available! | Not available! | Integer |  | 1 |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

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
			[1,  datetime.datetime.fromtimestamp(1), "10.0"],
			[1,  datetime.datetime.fromtimestamp(2), "11.0"],
			[1,  datetime.datetime.fromtimestamp(3), "12.0"],
			[1,  datetime.datetime.fromtimestamp(4), "13.0"],
			[1,  datetime.datetime.fromtimestamp(5), "14.0"],
			[1,  datetime.datetime.fromtimestamp(6), "15.0"],
			[1,  datetime.datetime.fromtimestamp(7), "16.0"],
			[1,  datetime.datetime.fromtimestamp(8), "17.0"],
			[1,  datetime.datetime.fromtimestamp(9), "18.0"],
			[1,  datetime.datetime.fromtimestamp(10), "19.0"]
])

source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val string', op_type='stream')

source.link(
			OverCountWindowStreamOp()
				.setPartitionCols(["id"])
				.setTimeCol("ts")
				.setPrecedingRows(5)
				.setClause("mtable_agg_preceding(ts, val) as data")
		).link(
			ShiftStreamOp()
				.setValueCol("data")
				.setShiftNum(7)
				.setPredictNum(12)
				.setPredictionCol("predict")
		).link(
			LookupVectorInTimeSeriesStreamOp()
				.setTimeCol("ts")
				.setTimeSeriesCol("predict")
				.setOutputCol("out")
		).print()

StreamOperator.execute()
```
### Java 代码
```java
package com.alibaba.alink.operator.stream.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.dataproc.GroupDataBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.timeseries.ShiftBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class ShiftStreamOpTest {

	@Test
	public void test() throws Exception {

		List <Row> mTableData = Arrays.asList(
			Row.of(1, new Timestamp(1), 10.0),
			Row.of(1, new Timestamp(2), 11.0),
			Row.of(1, new Timestamp(3), 12.0),
			Row.of(1, new Timestamp(4), 13.0),
			Row.of(1, new Timestamp(5), 14.0),
			Row.of(1, new Timestamp(6), 15.0),
			Row.of(1, new Timestamp(7), 16.0),
			Row.of(1, new Timestamp(8), 17.0),
			Row.of(1, new Timestamp(9), 18.0),
			Row.of(1, new Timestamp(10), 19.0)
		);

		MemSourceStreamOp source = new MemSourceStreamOp(mTableData, new String[] {"id", "ts", "val"});

		source.link(
			new OverCountWindowStreamOp()
				.setPartitionCols("id")
				.setTimeCol("ts")
				.setPrecedingRows(5)
				.setClause("mtable_agg(ts, val) as data")
		).link(
			new ShiftStreamOp()
				.setGroupCol("id")
				.setValueCol("data")
				.setShiftNum(7)
				.setPredictNum(12)
				.setPredictionCol("predict")
		).link(
			new LookupValueInTimeSeriesStreamOp()
				.setTimeCol("ts")
				.setTimeSeriesCol("predict")
				.setOutputCol("out")
		).print();

		StreamOperator.execute();
	}

}
```

### 运行结果
id|ts|val|data|predict|out
---|---|---|----|-------|---
1|1970-01-01 08:00:00.001|10.0000|{"data":{"ts":["1970-01-01 08:00:00.001"],"val":[10.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null|null
1|1970-01-01 08:00:00.002|11.0000|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002"],"val":[10.0,11.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.003","1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01","1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014"],"val":[10.0,11.0,10.0,11.0,10.0,11.0,10.0,11.0,10.0,11.0,10.0,11.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null
1|1970-01-01 08:00:00.003|12.0000|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003"],"val":[10.0,11.0,12.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01","1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015"],"val":[10.0,11.0,12.0,10.0,11.0,12.0,10.0,11.0,12.0,10.0,11.0,12.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null
1|1970-01-01 08:00:00.004|13.0000|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003","1970-01-01 08:00:00.004"],"val":[10.0,11.0,12.0,13.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01","1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016"],"val":[10.0,11.0,12.0,13.0,10.0,11.0,12.0,13.0,10.0,11.0,12.0,13.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null
1|1970-01-01 08:00:00.005|14.0000|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003","1970-01-01 08:00:00.004","1970-01-01 08:00:00.005"],"val":[10.0,11.0,12.0,13.0,14.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01","1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016","1970-01-01 08:00:00.017"],"val":[10.0,11.0,12.0,13.0,14.0,10.0,11.0,12.0,13.0,14.0,10.0,11.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null
1|1970-01-01 08:00:00.006|15.0000|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003","1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006"],"val":[10.0,11.0,12.0,13.0,14.0,15.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01","1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016","1970-01-01 08:00:00.017","1970-01-01 08:00:00.018"],"val":[10.0,11.0,12.0,13.0,14.0,15.0,10.0,11.0,12.0,13.0,14.0,15.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null
1|1970-01-01 08:00:00.007|16.0000|{"data":{"ts":["1970-01-01 08:00:00.002","1970-01-01 08:00:00.003","1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007"],"val":[11.0,12.0,13.0,14.0,15.0,16.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01","1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016","1970-01-01 08:00:00.017","1970-01-01 08:00:00.018","1970-01-01 08:00:00.019"],"val":[11.0,12.0,13.0,14.0,15.0,16.0,11.0,12.0,13.0,14.0,15.0,16.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null
1|1970-01-01 08:00:00.008|17.0000|{"data":{"ts":["1970-01-01 08:00:00.003","1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008"],"val":[12.0,13.0,14.0,15.0,16.0,17.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.009","1970-01-01 08:00:00.01","1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016","1970-01-01 08:00:00.017","1970-01-01 08:00:00.018","1970-01-01 08:00:00.019","1970-01-01 08:00:00.02"],"val":[12.0,13.0,14.0,15.0,16.0,17.0,12.0,13.0,14.0,15.0,16.0,17.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null
1|1970-01-01 08:00:00.009|18.0000|{"data":{"ts":["1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009"],"val":[13.0,14.0,15.0,16.0,17.0,18.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.01","1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016","1970-01-01 08:00:00.017","1970-01-01 08:00:00.018","1970-01-01 08:00:00.019","1970-01-01 08:00:00.02","1970-01-01 08:00:00.021"],"val":[13.0,14.0,15.0,16.0,17.0,18.0,13.0,14.0,15.0,16.0,17.0,18.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null
1|1970-01-01 08:00:00.01|19.0000|{"data":{"ts":["1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01"],"val":[14.0,15.0,16.0,17.0,18.0,19.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016","1970-01-01 08:00:00.017","1970-01-01 08:00:00.018","1970-01-01 08:00:00.019","1970-01-01 08:00:00.02","1970-01-01 08:00:00.021","1970-01-01 08:00:00.022"],"val":[14.0,15.0,16.0,17.0,18.0,19.0,14.0,15.0,16.0,17.0,18.0,19.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null
