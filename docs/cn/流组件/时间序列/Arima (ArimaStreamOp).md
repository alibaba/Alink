# Arima (ArimaStreamOp)
Java 类名：com.alibaba.alink.operator.stream.timeseries.ArimaStreamOp

Python 类名：ArimaStreamOp


## 功能介绍
使用Arima进行时间序列预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| order | 模型(p, d, q) | 模型(p, d, q) | int[] | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| valueCol | value列，类型为MTable | value列，类型为MTable | String | ✓ |  |
| seasonalOrder | 季节模型(p, d, q) | 季节模型(p, d, q) | int[] |  | null |
| estMethod | 估计方法 | 估计方法 | String |  | "CssMle" |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| seasonalPeriod | 季节周期 | 季节周期 | Integer |  | 1 |
| predictNum | 预测条数 | 预测条数 | Integer |  | 1 |
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

source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double', op_type='stream')

source.link(
			OverCountWindowStreamOp()
				.setPartitionCols(["id"])
				.setTimeCol("ts")
				.setPrecedingRows(5)
				.setClause("mtable_agg_preceding(ts, val) as data")
		).link(
			ArimaStreamOp()
				.setValueCol("data")
				.setOrder([1, 2, 1])
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

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class ArimaStreamOpTest extends AlinkTestBase {

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
			new ArimaStreamOp()
				.setValueCol("data")
				.setOrder(new int[] {1, 2, 1})
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
1|1970-01-01 08:00:00.002|11.0000|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002"],"val":[10.0,11.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null|null
