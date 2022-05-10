# DeepAR预测 (DeepARPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.timeseries.DeepARPredictBatchOp

Python 类名：DeepARPredictBatchOp


## 功能介绍
使用 DeepAR 进行时间序列训练和预测。

### 使用方式

参考文档 https://www.yuque.com/pinshu/alink_guide/xbp5ky

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
    [0,  datetime.datetime.fromisoformat('2021-11-01 00:00:00'), 100.0],
    [0,  datetime.datetime.fromisoformat('2021-11-02 00:00:00'), 100.0],
    [0,  datetime.datetime.fromisoformat('2021-11-03 00:00:00'), 100.0],
    [0,  datetime.datetime.fromisoformat('2021-11-04 00:00:00'), 100.0],
    [0,  datetime.datetime.fromisoformat('2021-11-05 00:00:00'), 100.0]
])

source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='batch')

deepARTrainBatchOp = DeepARTrainBatchOp()\
    .setTimeCol("ts")\
    .setSelectedCol("series")\
    .setNumEpochs(10)\
    .setWindow(2)\
    .setStride(1)

groupByBatchOp = GroupByBatchOp()\
    .setGroupByPredicate("id")\
    .setSelectClause("mtable_agg(ts, series) as mtable_agg_series")

deepARPredictBatchOp = DeepARPredictBatchOp()\
            .setPredictNum(2)\
            .setPredictionCol("pred")\
            .setValueCol("mtable_agg_series")

deepARPredictBatchOp\
    .linkFrom(
        deepARTrainBatchOp.linkFrom(source),
        groupByBatchOp.linkFrom(source)
    )\
    .print()
```
### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.operator.batch.timeseries.DeepARPredictBatchOp;
import com.alibaba.alink.operator.batch.timeseries.DeepARTrainBatchOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class DeepARTrainBatchOpTest {

	@Test
	public void testDeepARTrainBatchOp() throws Exception {
		BatchOperator.setParallelism(1);

		List <Row> data = Arrays.asList(
			Row.of(0, Timestamp.valueOf("2021-11-01 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-02 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-03 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-04 00:00:00"), 100.0),
			Row.of(0, Timestamp.valueOf("2021-11-05 00:00:00"), 100.0)
		);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "id int, ts timestamp, series double");

		DeepARTrainBatchOp deepARTrainBatchOp = new DeepARTrainBatchOp()
			.setTimeCol("ts")
			.setSelectedCol("series")
			.setNumEpochs(10)
			.setWindow(2)
			.setStride(1);

		GroupByBatchOp groupByBatchOp = new GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("mtable_agg(ts, series) as mtable_agg_series");

		DeepARPredictBatchOp deepARPredictBatchOp = new DeepARPredictBatchOp()
			.setPredictNum(2)
			.setPredictionCol("pred")
			.setValueCol("mtable_agg_series");

		deepARPredictBatchOp
			.linkFrom(
				deepARTrainBatchOp.linkFrom(memSourceBatchOp),
				groupByBatchOp.linkFrom(memSourceBatchOp)
			)
			.print();
	}
}
```

### 运行结果
| id | mtable_agg_series                                                                                                                                                                                                        | pred                                                                                                                                                    |
|----+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | {"data":{"ts":["2021-11-01 00:00:00.0","2021-11-02 00:00:00.0","2021-11-03 00:00:00.0","2021-11-04 00:00:00.0","2021-11-05 00:00:00.0"],"series":[100.0,100.0,100.0,100.0,100.0]},"schema":"ts TIMESTAMP,series DOUBLE"} | {"data":{"ts":["2021-11-06 00:00:00.0","2021-11-07 00:00:00.0"],"series":[31.424224853515625,39.10265350341797]},"schema":"ts TIMESTAMP,series DOUBLE"} |
