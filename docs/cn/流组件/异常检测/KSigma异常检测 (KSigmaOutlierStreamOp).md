# KSigma异常检测 (KSigmaOutlierStreamOp)
Java 类名：com.alibaba.alink.operator.stream.outlier.KSigmaOutlierStreamOp

Python 类名：KSigmaOutlierStreamOp


## 功能介绍
KSigma算法是一种常用的异常检测算法,如果整体数据服从正态分布，则如果一个点偏离均值K倍标准差，则该点被视为异常点.

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| precedingTime | 时间窗口大小 | 时间窗口大小 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| direction | Not available! | Not available! | String |  | "POSITIVE", "NEGATIVE", "BOTH" | "BOTH" |
| featureCol | 特征列名 | 特征列名，默认选最左边的列 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| precedingRows | 数据窗口大小 | 数据窗口大小 | Integer |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String |  |  | null |
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
			[1, datetime.datetime.fromtimestamp(1), 10.0, 0],
			[1, datetime.datetime.fromtimestamp(2), 11.0, 0],
			[1, datetime.datetime.fromtimestamp(3), 12.0, 0],
			[1, datetime.datetime.fromtimestamp(4), 13.0, 0],
			[1, datetime.datetime.fromtimestamp(5), 14.0, 0],
			[1, datetime.datetime.fromtimestamp(6), 15.0, 0],
			[1, datetime.datetime.fromtimestamp(7), 16.0, 0],
			[1, datetime.datetime.fromtimestamp(8), 17.0, 0],
			[1, datetime.datetime.fromtimestamp(9), 18.0, 0],
			[1, datetime.datetime.fromtimestamp(10), 19.0, 0]
])

dataOp = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double, label int', op_type='stream')

outlierOp = KSigmaOutlierStreamOp()\
			.setGroupCols(["id"])\
			.setTimeCol("ts")\
			.setPrecedingRows(3)\
			.setFeatureCol("val")\
			.setPredictionCol("pred")\
			.setPredictionDetailCol("pred_detail")

dataOp.link(outlierOp).print()

StreamOperator.execute()
```

### java示例

```java
package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class KSigmaOutlierStreamOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, new Timestamp(1), 10.0, 0),
			Row.of(1, new Timestamp(2), 11.0, 0),
			Row.of(1, new Timestamp(3), 12.0, 0),
			Row.of(1, new Timestamp(4), 13.0, 0),
			Row.of(1, new Timestamp(5), 14.0, 0),
			Row.of(1, new Timestamp(6), 15.0, 0),
			Row.of(1, new Timestamp(7), 16.0, 0),
			Row.of(1, new Timestamp(8), 17.0, 0),
			Row.of(1, new Timestamp(9), 18.0, 0),
			Row.of(1, new Timestamp(10), 19.0, 0)
		);

		MemSourceStreamOp dataOp = new MemSourceStreamOp(mTableData, new String[] {"id", "ts", "val", "label"});

		KSigmaOutlierStreamOp outlierOp = new KSigmaOutlierStreamOp()
			.setGroupCols("id")
			.setTimeCol("ts")
			.setPrecedingRows(3)
			.setFeatureCol("val")
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		dataOp.link(outlierOp).print();

		StreamOperator.execute();

	}

}
```
### 运行结果

无

