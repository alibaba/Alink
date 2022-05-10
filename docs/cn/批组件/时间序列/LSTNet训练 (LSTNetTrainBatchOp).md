# LSTNet训练 (LSTNetTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.timeseries.LSTNetTrainBatchOp

Python 类名：LSTNetTrainBatchOp


## 功能介绍
使用 LSTNet 进行时间序列训练和预测。

### 使用方式

参考文档 https://www.yuque.com/pinshu/alink_guide/xbp5ky

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| checkpointFilePath | 保存 checkpoint 的路径 | 用于保存中间结果的路径，将作为 TensorFlow 中 `Estimator` 的 `model_dir` 传入，需要为所有 worker 都能访问到的目录 | String | ✓ |  |  |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String | ✓ | 所选列类型为 [TIMESTAMP] |  |
| batchSize | 数据批大小 | 数据批大小 | Integer |  |  | 128 |
| horizon | horizon大小 | horizon大小 | Integer |  | [1, +inf) | 12 |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  |  | 4 |
| learningRate | 学习率 | 学习率 | Double |  |  | 0.001 |
| numEpochs | epoch数 | epoch数 | Integer |  |  | 10 |
| numPSs | PS 角色数 | PS 角色的数量。值未设置时，如果 Worker 角色数也未设置，则为作业总并发度的 1/4（需要取整），否则为总并发度减去 Worker 角色数。 | Integer |  |  | null |
| numWorkers | Worker 角色数 | Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。 | Integer |  |  | null |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；如果是目录，那么只能使用本地路径，即 file://。 | String |  |  | "" |
| removeCheckpointBeforeTraining | 是否在训练前移除 checkpoint 相关文件 | 是否在训练前移除 checkpoint 相关文件用于重新训练，只会删除必要的文件 | Boolean |  |  | null |
| selectedCol | 计算列对应的列名 | 计算列对应的列名, 默认值是null | String |  |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| window | 窗口大小 | 窗口大小 | Integer |  |  | 5 |

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
