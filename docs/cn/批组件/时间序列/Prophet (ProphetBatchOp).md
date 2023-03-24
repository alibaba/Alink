# Prophet (ProphetBatchOp)
Java 类名：com.alibaba.alink.operator.batch.timeseries.ProphetBatchOp

Python 类名：ProphetBatchOp


## 功能介绍
对每一行的MTable数据, 进行Prophet时间序列预测，给出下一时间段的预测结果。

### 使用方式

参考文档 https://www.yuque.com/pinshu/alink_guide/xbp5ky

### 算法原理

Prophet是facebook开源的一个时间序列预测算法, github地址：https://github.com/facebook/prophet.

Prophet适用于具有明显的内在规律的数据, 例如：

* 有一定的历史数据，有至少几个月的每小时、每天或每周观察的历史数据
* 有较强的季节性趋势：每周的一些天，每年的一些时间
* 有已知的以不定期的间隔发生的重要节假日（比如国庆节）
* 缺失的历史数据或较大的异常数据的数量在合理范围内
* 对于数据中蕴含的非线性增长的趋势都有一个自然极限或饱和状态

### 使用方式
* 第一步，将每组数据(时间列和数据列) 聚合成MTable.
    ```python
     GroupByBatchOp()
        .setGroupByPredicate("id")
        .setSelectClause("id, mtable_agg(ts, val) as data")
    ```
* 第二步，使用时间序列方法进行预测，预测结果也是MTable。
* 第三步，使用FlattenMTableBatchOp，将MTable转换成列，
   ```python
      FlattenMTableBatchOp()
          .setReservedCols(["id", "predict"])
          .setSelectedCol("predict")
          .setSchemaStr("ts timestamp, val double")
   ```
  
## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| valueCol | value列，类型为MTable | value列，类型为MTable | String | ✓ | 所选列类型为 [M_TABLE, STRING] |  |
| cap | cap | cap | Double |  |  | null |
| changePointPriorScale | changepoint_prior_scale | changepoint_prior_scale | Double |  |  | 0.05 |
| changePointRange | change_point_range | change_point_range | Double |  |  | 0.8 |
| changePoints | changepoints | changepoints | String |  |  | null |
| dailySeasonality | daily_seasonality | daily_seasonality | String |  |  | "auto" |
| floor | floor | floor | Double |  |  | null |
| growth | growth | growth | String |  | "LINEAR", "LOGISTIC", "FLAT" | "LINEAR" |
| holidays | 节假日 | 节假日，格式是 playoff:2008-01-13,2009-01-03 superbowl: 2010-02-07,2014-02-02 | String |  |  | null |
| holidaysPriorScale | holidays_prior_scale | holidays_prior_scale | Double |  |  | 10.0 |
| includeHistory | include_history | include_history | Boolean |  |  | false |
| intervalWidth | interval_width | interval_width | Double |  |  | 0.8 |
| mcmcSamples | mcmc_samples | mcmc_samples | Integer |  |  | 0 |
| nChangePoint | n_change_point | n_change_point | Integer |  |  | 25 |
| predictNum | 预测条数 | 预测条数 | Integer |  |  | 1 |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| seasonalityMode | seasonality_mode | seasonality_mode | String |  | "MULTIPLICATIVE", "ADDITIVE" | "ADDITIVE" |
| seasonalityPriorScale | seasonality_prior_scale | seasonality_prior_scale | Double |  |  | 10.0 |
| stanInit | 初始值 | 初始值 | String |  |  | null |
| uncertaintySamples | 用来计算指标的采样数目 | 用来计算指标的采样数目，设置成0，不计算指标。 | Integer |  |  | 1000 |
| weeklySeasonality | weekly_seasonality | weekly_seasonality | String |  |  | "auto" |
| yearlySeasonality | yearly_seasonality | yearly_seasonality | String |  |  | "auto" |
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

downloader = AlinkGlobalConfiguration.getPluginDownloader()
downloader.downloadPlugin('tf115_python_env_linux')

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
		).link(ProphetBatchOp()
			.setValueCol("data")
			.setPredictNum(4)
			.setPredictionCol("pred")
		).print()
```
### Java 代码
```java
package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class ArimaBatchOpTest extends AlinkTestBase {

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

		MemSourceBatchOp source = new MemSourceBatchOp(mTableData, new String[] {"id", "ts", "val"});

		source.link(
			new GroupByBatchOp()
				.setGroupByPredicate("id")
				.setSelectClause("mtable_agg(ts, val) as data")
		).link(new ProphetBatchOp()
               	.setValueCol("data")
               	.setPredictNum(4)
               	.setPredictionCol("pred")
		).print();
	}
}
```

### 运行结果
id|data|predict
---|----|-------
1|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003","1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01"],"val":[10.0,11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016","1970-01-01 08:00:00.017","1970-01-01 08:00:00.018","1970-01-01 08:00:00.019","1970-01-01 08:00:00.02","1970-01-01 08:00:00.021","1970-01-01 08:00:00.022"],"val":[20.0,21.0,22.0,23.0,24.0,25.0,26.0,27.0,28.0,29.0,30.0,31.0]},"schema":"ts TIMESTAMP,val DOUBLE"}
t
