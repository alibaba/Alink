# 时间序列评估 (EvalTimeSeriesBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalTimeSeriesBatchOp

Python 类名：EvalTimeSeriesBatchOp


## 功能介绍
对时间序列结果进行评估。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |

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
			[1, datetime.datetime.fromtimestamp(1), 10.0, 10.5],
			[1, datetime.datetime.fromtimestamp(2), 11.0, 10.5],
			[1, datetime.datetime.fromtimestamp(3), 12.0, 11.5],
			[1, datetime.datetime.fromtimestamp(4), 13.0, 12.5],
			[1, datetime.datetime.fromtimestamp(5), 14.0, 13.5],
			[1, datetime.datetime.fromtimestamp(6), 15.0, 14.5],
			[1, datetime.datetime.fromtimestamp(7), 16.0, 14.5],
			[1, datetime.datetime.fromtimestamp(8), 17.0, 14.5],
			[1, datetime.datetime.fromtimestamp(9), 18.0, 14.5],
			[1, datetime.datetime.fromtimestamp(10), 19.0, 16.5]
])

source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double, pred double', op_type='batch')

cmex = source.link(
    EvalTimeSeriesBatchOp()\
        .setLabelCol("val")\
        .setPredictionCol("pred")
).collectMetrics()

print(cmex.getMse())
print(cmex.getMae())
print(cmex.getRmse())
print(cmex.getSse())
print(cmex.getSst())
print(cmex.getSsr())
print(cmex.getSae())
print(cmex.getMape())
print(cmex.getSmape())
print(cmex.getND())
print(cmex.getCount())
print(cmex.getYMean())
print(cmex.getPredictionMean())
```

### Java 代码
```java
package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;


public class EvalTimeSeriesBatchOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, new Timestamp(1), 10.0, 10.5),
			Row.of(1, new Timestamp(2), 11.0, 10.5),
			Row.of(1, new Timestamp(3), 12.0, 11.5),
			Row.of(1, new Timestamp(4), 13.0, 12.5),
			Row.of(1, new Timestamp(5), 14.0, 13.5),
			Row.of(1, new Timestamp(6), 15.0, 14.5),
			Row.of(1, new Timestamp(7), 16.0, 14.5),
			Row.of(1, new Timestamp(8), 17.0, 14.5),
			Row.of(1, new Timestamp(9), 18.0, 14.5),
			Row.of(1, new Timestamp(10), 19.0, 16.5)
		);

		MemSourceBatchOp source = new MemSourceBatchOp(mTableData, new String[] {"id", "ts", "val", "pred"});

		source.link(
			new EvalTimeSeriesBatchOp()
				.setLabelCol("val")
				.setPredictionCol("pred")
		).lazyPrintMetrics();

		BatchOperator.execute();
	}

}
```

### 运行结果

|regression_eval_result|
|----------------------|
|{"predictionMean":"13.3","SSE":"28.5","count":"10.0","SMAPE":"8.606434351991227","MAPE":"8.114625849726469","RMSE":"1.6881943016134133","MAE":"1.3","SSR":"50.0","yMean":"14.5","SST":"82.5","SAE":"13.0","ND":"0.0896551724137931","Explained Variance":"5.0","MSE":"2.85"}|
