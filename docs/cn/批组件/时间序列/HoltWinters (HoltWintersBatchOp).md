# HoltWinters (HoltWintersBatchOp)
Java 类名：com.alibaba.alink.operator.batch.timeseries.HoltWintersBatchOp

Python 类名：HoltWintersBatchOp


## 功能介绍
使用HoltWinters进行时间序列预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| valueCol | Not available! | Not available! | String | ✓ |  |
| alpha | Not available! | Not available! | Double |  | 0.3 |
| beta | Not available! | Not available! | Double |  | 0.1 |
| doSeasonal | Not available! | Not available! | Boolean |  | false |
| doTrend | Not available! | Not available! | Boolean |  | false |
| frequency | Not available! | Not available! | Integer |  | 10 |
| gamma | Not available! | Not available! | Double |  | 0.1 |
| levelStart | Not available! | Not available! | Double |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| seasonalStart | Not available! | Not available! | double[] |  |  |
| seasonalType | Not available! | Not available! | String |  | "ADDITIVE" |
| trendStart | Not available! | Not available! | Double |  |  |
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
		).link(HoltWintersBatchOp()
			.setValueCol("data")
			.setPredictionCol("pred")
			.setPredictNum(12)
		).print()
```
### Java 代码
```java
package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class HoltWintersBatchOpTest {
	@Test
	public void test() throws Exception {
		List<Row> mTableData = Arrays.asList(
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

		MemSourceBatchOp source = new MemSourceBatchOp(mTableData, new String[] {"id", "ts", "val"});

		source.link(
			new GroupByBatchOp()
				.setGroupByPredicate("id")
				.setSelectClause("mtable_agg(ts, val) as data")
		).link(new HoltWintersBatchOp()
			.setValueCol("data")
			.setPredictionCol("pred")
			.setPredictNum(12)
		).print();
	}
}
```

### 运行结果
id|data|pred
---|----|----
1|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003","1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01"],"val":[10.0,11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016","1970-01-01 08:00:00.017","1970-01-01 08:00:00.018","1970-01-01 08:00:00.019","1970-01-01 08:00:00.02","1970-01-01 08:00:00.021","1970-01-01 08:00:00.022"],"val":[19.0,19.0,19.0,19.0,19.0,19.0,19.0,19.0,19.0,19.0,19.0,19.0]},"schema":"ts TIMESTAMP,val DOUBLE"}
