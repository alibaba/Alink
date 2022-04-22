# 时间序列插值 (LookupValueInTimeSeriesStreamOp)
Java 类名：com.alibaba.alink.operator.stream.timeseries.LookupValueInTimeSeriesStreamOp

Python 类名：LookupValueInTimeSeriesStreamOp


## 功能介绍
在时间序列中查找对应时间的值。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String | ✓ | 所选列类型为 [TIMESTAMP] |  |
| timeSeriesCol | 时间序列列 | 时间序列列，是特殊的MTable类型，一列是时间，一列是值 | String | ✓ | 所选列类型为 [M_TABLE] |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

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

source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double', op_type='stream')

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
			LookupValueInTimeSeriesStreamOp()
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

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.timeseries.LookupValueInTimeSeriesBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class LookupValueInTimeSeriesStreamOpTest {
	@Test
	public void test() throws Exception {

		List <Row> mTableData = Arrays.asList(
			Row.of(new Timestamp(1), 10.0),
			Row.of(new Timestamp(2), 11.0),
			Row.of(new Timestamp(3), 12.0),
			Row.of(new Timestamp(4), 13.0),
			Row.of(new Timestamp(5), 14.0),
			Row.of(new Timestamp(6), 15.0),
			Row.of(new Timestamp(7), 16.0),
			Row.of(new Timestamp(8), 17.0),
			Row.of(new Timestamp(9), 18.0),
			Row.of(new Timestamp(10), 19.0)
		);

		MTable mtable = new MTable(mTableData, "ts timestamp, val double");

		MemSourceStreamOp source = new MemSourceStreamOp(
			new Object[][] {
				{1, new Timestamp(5), mtable}
			},
			new String[] {"id", "ts", "data"});

		source
			.link(new LookupValueInTimeSeriesStreamOp()
				.setTimeCol("ts")
				.setTimeSeriesCol("data")
				.setOutputCol("out")
			)
			.print();

		StreamOperator.execute();
	}
}
```

### 运行结果
id|ts|data|out
---|---|----|---
1|1970-01-01 08:00:00.005|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003","1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01"],"val":[10.0,11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|14.0000
