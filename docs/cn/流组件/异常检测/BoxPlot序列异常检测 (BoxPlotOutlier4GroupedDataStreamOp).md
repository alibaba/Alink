# BoxPlot序列异常检测 (BoxPlotOutlier4GroupedDataStreamOp)
Java 类名：com.alibaba.alink.operator.stream.outlier.BoxPlotOutlier4GroupedDataStreamOp

Python 类名：BoxPlotOutlier4GroupedDataStreamOp


## 功能介绍
BoxPlot算法是一种常用的异常检测算法.
BoxPlotOutlier4Series输入是MTable, 输出也是MTable, 返回序列数据(MTable)的所有异常点。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| inputMTableCol | 输入列名 | 输入序列的列名 | String | ✓ |  |  |
| outputMTableCol | 输出列名 | 输出序列的列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| direction | 方向 | 检测异常的方向 | String |  | "POSITIVE", "NEGATIVE", "BOTH" | "BOTH" |
| featureCol | 特征列名 | 特征列名，默认选最左边的列 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| maxOutlierNumPerGroup | 每组最大异常点数目 | 每组最大异常点数目 | Integer |  |  |  |
| maxOutlierRatio | 最大异常点比例 | 算法检测异常点的最大比例 | Double |  |  |  |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
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

dataOp.link(\
			OverCountWindowStreamOp()\
				.setGroupCols(["id"])\
				.setTimeCol("ts")\
				.setPrecedingRows(5)\
				.setClause("MTABLE_AGG_PRECEDING(ts, val) as series_data")\
				.setReservedCols(["id", "label"])\
		).link(\
			BoxPlotOutlier4GroupedDataStreamOp()\
				.setInputMTableCol("series_data")\
				.setFeatureCol("val")\
				.setOutputMTableCol("output_series")\
				.setPredictionCol("pred")\
		).link(\
			FlattenMTableStreamOp()\
				.setSelectedCol("output_series")\
				.setSchemaStr("ts TIMESTAMP, val DOUBLE, pred BOOLEAN")\
		).print()


StreamOperator.execute()
```

### Java 代码

```java
package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.FlattenMTableStreamOp;
import com.alibaba.alink.operator.stream.feature.OverCountWindowStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class BoxPlotOutlier4SeriesStreamOpTest extends TestCase {
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
		MemSourceStreamOp dataOp = new MemSourceStreamOp(mTableData,
			new String[] {"id", "ts", "val", "label"});

		dataOp.link(
			new OverCountWindowStreamOp()
				.setGroupCols("id")
				.setTimeCol("ts")
				.setPrecedingRows(5)
				.setClause("MTABLE_AGG_PRECEDING(ts, val) as series_data")
				.setReservedCols("id", "label")
		).link(
			new BoxPlotOutlier4GroupedDataStreamOp()
				.setInputMTableCol("series_data")
				.setFeatureCol("val")
				.setOutputMTableCol("output_series")
				.setPredictionCol("pred")
		).link(
			new FlattenMTableStreamOp()
				.setSelectedCol("output_series")
				.setSchemaStr("ts TIMESTAMP, val DOUBLE, pred BOOLEAN")
		).print();

		StreamOperator.execute();
	}
}
```

### 运行结果

无


