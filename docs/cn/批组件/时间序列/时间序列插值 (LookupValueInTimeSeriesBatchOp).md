# 时间序列插值 (LookupValueInTimeSeriesBatchOp)
Java 类名：com.alibaba.alink.operator.batch.timeseries.LookupValueInTimeSeriesBatchOp

Python 类名：LookupValueInTimeSeriesBatchOp


## 功能介绍
在时间序列中查找对应时间的值。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String | ✓ |  |
| timeSeriesCol | Not available! | Not available! | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
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

source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double', op_type='batch')

source.link(
        GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("id, mtable_agg(ts, val) as data")
		).link(
            ShiftBatchOp()
					.setValueCol("data")
					.setShiftNum(7)
					.setPredictNum(12)
					.setPredictionCol("predict")
		).link(
            FlattenMTableBatchOp()
					.setReservedCols(["id", "predict"])
					.setSelectedCol("predict")
					.setSchemaStr("ts timestamp, val double")
        ).link(
            LookupValueInTimeSeriesBatchOp()
				.setTimeCol("ts")
				.setTimeSeriesCol("predict")
				.setOutputCol("out")
				.setReservedCols(["id","ts"])
        ).print()
```
### Java 代码
```java
package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class LookupValueInTimeSeriesBatchOpTest {
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

		MemSourceBatchOp source = new MemSourceBatchOp(
			new Object[][] {
				{1, new Timestamp(5), mtable}
			},
			new String[] {"id", "ts", "data"});

		source
			.link(new LookupValueInTimeSeriesBatchOp()
				.setTimeCol("ts")
				.setTimeSeriesCol("data")
				.setOutputCol("out")
				.setReservedCols("id","ts")
			)
			.print();
	}
}
```

### 运行结果

|id|ts|out|
|:----:|:----:|:----:|
|1|	1970-01-01 08:00:11	|13.0|
|1|	1970-01-01 08:00:12	|14.0|
|1|	1970-01-01 08:00:13	|15.0|
|1|	1970-01-01 08:00:14	|16.0|
|1|	1970-01-01 08:00:15	|17.0|
|1|	1970-01-01 08:00:16	|18.0|
|1|	1970-01-01 08:00:17	|19.0|
|1|	1970-01-01 08:00:18	|13.0|
|1|	1970-01-01 08:00:19	|14.0|
|1|	1970-01-01 08:00:20	|15.0|
|1|	1970-01-01 08:00:21	|16.0|
|1|	1970-01-01 08:00:22	|17.0|
