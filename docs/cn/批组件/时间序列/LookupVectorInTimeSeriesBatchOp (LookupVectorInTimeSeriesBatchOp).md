# LookupVectorInTimeSeriesBatchOp (LookupVectorInTimeSeriesBatchOp)
Java 类名：com.alibaba.alink.operator.batch.timeseries.LookupVectorInTimeSeriesBatchOp

Python 类名：LookupVectorInTimeSeriesBatchOp


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
			[1,  datetime.datetime.fromtimestamp(1), "10.0 10.0"],
			[1,  datetime.datetime.fromtimestamp(2), "11.0 11.0"],
			[1,  datetime.datetime.fromtimestamp(3), "12.0 12.0"],
			[1,  datetime.datetime.fromtimestamp(4), "13.0 13.0"],
			[1,  datetime.datetime.fromtimestamp(5), "14.0 14.0"],
			[1,  datetime.datetime.fromtimestamp(6), "15.0 15.0"],
			[1,  datetime.datetime.fromtimestamp(7), "16.0 16.0"],
			[1,  datetime.datetime.fromtimestamp(8), "17.0 17.0"],
			[1,  datetime.datetime.fromtimestamp(9), "18.0 18.0"],
			[1,  datetime.datetime.fromtimestamp(10), "19.0 19.0"]
])

source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val string', op_type='batch')

source.link(
            GroupDataBatchOp()
				.setGroupCols(["id"])
				.setSelectedCols(["ts", "val"])
				.setOutputCol("data")
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
					.setSchemaStr("ts timestamp, val VEC_TYPES_VECTOR")
        ).link(
            LookupVectorInTimeSeriesBatchOp()
				.setTimeCol("ts")
				.setTimeSeriesCol("predict")
				.setOutputCol("out")
				.setReservedCols(["id"])
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

import static org.junit.Assert.*;

public class LookupVectorInTimeSeriesBatchOpTest {
	@Test
	public void test() throws Exception {

		List<Row> mTableData = Arrays.asList(
			Row.of(new Timestamp(1), "10.0 21.0"),
			Row.of(new Timestamp(2), "11.0 22.0"),
			Row.of(new Timestamp(3), "12.0 23.0"),
			Row.of(new Timestamp(4), "13.0 24.0"),
			Row.of(new Timestamp(5), "14.0 25.0"),
			Row.of(new Timestamp(6), "15.0 26.0"),
			Row.of(new Timestamp(7), "16.0 27.0"),
			Row.of(new Timestamp(8), "17.0 28.0"),
			Row.of(new Timestamp(9), "18.0 29.0"),
			Row.of(new Timestamp(10), "19.0 30.0")
		);

		MTable mtable = new MTable(mTableData, "ts timestamp, val string");

		MemSourceBatchOp source = new MemSourceBatchOp(
			new Object[][] {
				{1, new Timestamp(5), mtable}
			},
			new String[] {"id", "ts", "data"});

		source
			.link(new LookupVectorInTimeSeriesBatchOp()
				.setTimeCol("ts")
				.setTimeSeriesCol("data")
				.setOutputCol("out")
			)
			.print();
	}
}
```

### 运行结果
   |id|	out  |
   |:----:|:----:|
|	1|	13.0 13.0 |
|	1|	14.0 14.0 |
|	1|	15.0 15.0 |
|	1|	16.0 16.0 |
|	1|	17.0 17.0 |
|	1|	18.0 18.0 |
|	1|	19.0 19.0 |
|	1|	13.0 13.0 |
|	1|	14.0 14.0 |
|	1|	15.0 15.0 |
|	1|	16.0 16.0 |
|	1|	17.0 17.0 |
