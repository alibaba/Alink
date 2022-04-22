# LSTNet预测 (LSTNetPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.timeseries.LSTNetPredictBatchOp

Python 类名：LSTNetPredictBatchOp


## 功能介绍
使用 LSTNet 进行时间序列训练和预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| valueCol | value列，类型为MTable | value列，类型为MTable | String | ✓ | 所选列类型为 [M_TABLE] |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
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
    [0, datetime.datetime.fromisoformat("2021-11-01 00:00:00"), 100.0],
    [0, datetime.datetime.fromisoformat("2021-11-02 00:00:00"), 200.0],
    [0, datetime.datetime.fromisoformat("2021-11-03 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-11-04 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-11-06 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-11-07 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-08 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-09 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-10 00:00:00"), 900.0],
    [0, datetime.datetime.fromisoformat("2021-11-11 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-12 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-13 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-14 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-11-15 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-11-16 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-11-17 00:00:00"), 200.0],
    [0, datetime.datetime.fromisoformat("2021-11-18 00:00:00"), 100.0],
    [0, datetime.datetime.fromisoformat("2021-11-19 00:00:00"), 200.0],
    [0, datetime.datetime.fromisoformat("2021-11-20 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-11-21 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-11-22 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-11-23 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-24 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-25 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-26 00:00:00"), 900.0],
    [0, datetime.datetime.fromisoformat("2021-11-27 00:00:00"), 800.0],
    [0, datetime.datetime.fromisoformat("2021-11-28 00:00:00"), 700.0],
    [0, datetime.datetime.fromisoformat("2021-11-29 00:00:00"), 600.0],
    [0, datetime.datetime.fromisoformat("2021-11-30 00:00:00"), 500.0],
    [0, datetime.datetime.fromisoformat("2021-12-01 00:00:00"), 400.0],
    [0, datetime.datetime.fromisoformat("2021-12-02 00:00:00"), 300.0],
    [0, datetime.datetime.fromisoformat("2021-12-03 00:00:00"), 200.0]
])

source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='batch')

lstNetTrainBatchOp = LSTNetTrainBatchOp()\
    .setTimeCol("ts")\
    .setSelectedCol("series")\
    .setNumEpochs(10)\
    .setWindow(24)\
    .setHorizon(1)

groupByBatchOp = GroupByBatchOp()\
    .setGroupByPredicate("id")\
    .setSelectClause("mtable_agg(ts, series) as mtable_agg_series")

lstNetPredictBatchOp = LSTNetPredictBatchOp()\
    .setPredictNum(1)\
    .setPredictionCol("pred")\
    .setReservedCols([])\
    .setValueCol("mtable_agg_series")\

lstNetPredictBatchOp\
    .linkFrom(
        lstNetTrainBatchOp.linkFrom(source),
        groupByBatchOp.linkFrom(source.filter("ts >= TO_TIMESTAMP('2021-11-10 00:00:00')"))
    )\
    .print()
```
### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.operator.batch.timeseries.LSTNetPredictBatchOp;
import com.alibaba.alink.operator.batch.timeseries.LSTNetTrainBatchOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class LSTNetTrainBatchOpTest {

	@Test
	public void testLSTNetTrainBatchOp() throws Exception {
		BatchOperator.setParallelism(1);

		List <Row> data = Arrays.asList(
			Row.of(0, Timestamp.valueOf("2021-11-01 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-02 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-03 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-04 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-06 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-07 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-08 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-09 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-10 00:00:00"), 900.0),
			Row.of(0, Timestamp.valueOf("2021-11-11 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-12 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-13 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-14 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-15 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-16 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-17 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-18 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-19 00:00:00"), 200.0),
			Row.of(0, Timestamp.valueOf("2021-11-20 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-11-21 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-11-22 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-11-23 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-24 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-25 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-26 00:00:00"), 900.0),
			Row.of(0, Timestamp.valueOf("2021-11-27 00:00:00"), 800.0),
			Row.of(0, Timestamp.valueOf("2021-11-28 00:00:00"), 700.0),
			Row.of(0, Timestamp.valueOf("2021-11-29 00:00:00"), 600.0),
			Row.of(0, Timestamp.valueOf("2021-11-30 00:00:00"), 500.0),
			Row.of(0, Timestamp.valueOf("2021-12-01 00:00:00"), 400.0),
			Row.of(0, Timestamp.valueOf("2021-12-02 00:00:00"), 300.0),
			Row.of(0, Timestamp.valueOf("2021-12-03 00:00:00"), 200.0)
		);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "id int, ts timestamp, series double");

		LSTNetTrainBatchOp lstNetTrainBatchOp = new LSTNetTrainBatchOp()
			.setTimeCol("ts")
			.setSelectedCol("series")
			.setNumEpochs(10)
			.setWindow(24)
			.setHorizon(1);

		GroupByBatchOp groupByBatchOp = new GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("mtable_agg(ts, series) as mtable_agg_series");

		LSTNetPredictBatchOp lstNetPredictBatchOp = new LSTNetPredictBatchOp()
			.setPredictNum(1)
			.setPredictionCol("pred")
			.setReservedCols()
			.setValueCol("mtable_agg_series");

		lstNetPredictBatchOp
			.linkFrom(
				lstNetTrainBatchOp.linkFrom(memSourceBatchOp),
				groupByBatchOp.linkFrom(memSourceBatchOp.filter("ts >= TO_TIMESTAMP('2021-11-10 00:00:00')"))
			)
			.print();
	}
}
```

### 运行结果
| pred                                                                                                          |
|---------------------------------------------------------------------------------------------------------------|
| {"data":{"ts":["2021-12-04 00:00:00.0"],"series":[441.76019287109375]},"schema":"ts TIMESTAMP,series DOUBLE"} |
