# auto Garch (AutoGarchStreamOp)
Java 类名：com.alibaba.alink.operator.stream.timeseries.AutoGarchStreamOp

Python 类名：AutoGarchStreamOp


## 功能介绍
给定分组，对每一组的数据使用AutoGarch进行时间序列预测。

### 算法原理

garch(Generalized AutoRegressive Conditional Heteroskedasticity) 又称广义自回归条件异方差模型, 

garch 详细介绍请见链接 https://en.wikipedia.org/wiki/Autoregressive_conditional_heteroskedasticity#GARCH

garch是只需要指定MaxOrder, 不需要指定p/d/q, 对每个分组分别计算出最优的参数，给出预测结果。

### 使用方式

参考文档 https://www.yuque.com/pinshu/alink_guide/xbp5ky


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| valueCol | value列，类型为MTable | value列，类型为MTable | String | ✓ | 所选列类型为 [M_TABLE] |  |
| icType | 评价指标 | 评价指标 | String |  | "AIC", "BIC", "HQIC" | "AIC" |
| ifGARCH11 | 是否用garch11 | 是否用garch11 | Boolean |  |  | true |
| maxOrder | 模型(p, q)上限 | 模型(p, q)上限 | Integer |  |  | 10 |
| minusMean | 是否减去均值 | 是否减去均值 | Boolean |  |  | true |
| predictNum | 预测条数 | 预测条数 | Integer |  |  | 1 |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
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
				.setGroupCols(["id"])
				.setTimeCol("ts")
				.setPrecedingRows(5)
				.setClause("mtable_agg_preceding(ts, val) as data")
		).link(
			AutoGarchStreamOp()
				.setValueCol("data")
				.setIcType("AIC")
				.setPredictNum(10)
				.setMaxOrder(4)
				.setIfGARCH11(False)
				.setMinusMean(False)
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

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.timeseries.AutoGarchBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class AutoGarchStreamOpTest extends AlinkTestBase {

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
				.setGroupCols("id")
				.setTimeCol("ts")
				.setPrecedingRows(5)
				.setClause("mtable_agg(ts, val) as data")
		).link(
			new AutoGarchStreamOp()
				.setValueCol("data")
				.setIcType("AIC")
				.setPredictNum(10)
				.setMaxOrder(4)
				.setIfGARCH11(false)
				.setMinusMean(false)
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
1|1970-01-01 08:00:00.003|12.0000|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003"],"val":[10.0,11.0,12.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null|null
1|1970-01-01 08:00:00.004|13.0000|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003","1970-01-01 08:00:00.004"],"val":[10.0,11.0,12.0,13.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|null|null
