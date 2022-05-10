# Prophet训练 (ProphetTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.timeseries.ProphetTrainBatchOp

Python 类名：ProphetTrainBatchOp


## 功能介绍
进行Prophet训练，得到Prophet模型，可以用来做预测。

### 算法原理

Prophet是facebook开源的一个时间序列预测算法, github地址：https://github.com/facebook/prophet.

Prophet适用于具有明显的内在规律的数据, 例如：

* 有一定的历史数据，有至少几个月的每小时、每天或每周观察的历史数据
* 有较强的季节性趋势：每周的一些天，每年的一些时间
* 有已知的以不定期的间隔发生的重要节假日（比如国庆节）
* 缺失的历史数据或较大的异常数据的数量在合理范围内
* 对于数据中蕴含的非线性增长的趋势都有一个自然极限或饱和状态

### 使用方式

参考文档 https://www.yuque.com/pinshu/alink_guide/xbp5ky

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String | ✓ | 所选列类型为 [TIMESTAMP] |  |
| valueCol | value列，类型为MTable | value列，类型为MTable | String | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| pythonEnv | Python 环境路径 | Python 环境路径，一般情况下不需要填写。如果是压缩文件，需要解压后得到一个目录，且目录名与压缩文件主文件名一致，可以使用 http://, https://, oss://, hdfs:// 等路径；如果是目录，那么只能使用本地路径，即 file://。 | String |  |  | "" |

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

prophetModel = source.link(\
                    ProphetTrainBatchOp()\
                        .setTimeCol("ts")\
                        .setValueCol("val")
                )

ProphetPredictBatchOp()\
    .setValueCol("data")\
    .setPredictNum(4)\
    .setPredictionCol("pred")\
    .linkFrom(
         prophetModel,
         source.link(
            GroupByBatchOp()
			    .setGroupByPredicate("id")
			    .setSelectClause("id, mtable_agg(ts, val) as data")
            )
    )\
    .print()
```
### Java 代码

```java
package com.alibaba.alink.operator.batch.timeseries;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;

public class ProphetBatchOpTest {

	@Test
	public void testModel() throws Exception {
		Row[] rowsData =
			new Row[] {
				Row.of("1", new Timestamp(117, 11, 1, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 2, 0, 0, 0, 0), 8.51959031601596),
				Row.of("2", new Timestamp(117, 11, 3, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 4, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 5, 0, 0, 0, 0), 8.51959031601596),
				Row.of("1", new Timestamp(117, 11, 6, 0, 0, 0, 0), 8.07246736935477),
				Row.of("2", new Timestamp(117, 11, 7, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 8, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 9, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 10, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 11, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 12, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 13, 0, 0, 0, 0), 8.18367658262066),
				Row.of("2", new Timestamp(117, 11, 14, 0, 0, 0, 0), 8.18367658262066),
				Row.of("1", new Timestamp(117, 11, 15, 0, 0, 0, 0), 7.8935720735049),
				Row.of("1", new Timestamp(117, 11, 16, 0, 0, 0, 0), 7.78364059622125),
				Row.of("2", new Timestamp(117, 11, 17, 0, 0, 0, 0), 8.07246736935477),
				Row.of("1", new Timestamp(117, 11, 18, 0, 0, 0, 0), 8.41405243249672),
				Row.of("1", new Timestamp(117, 11, 19, 0, 0, 0, 0), 8.82922635473185),
				Row.of("1", new Timestamp(117, 11, 20, 0, 0, 0, 0), 8.38251828808963),
				Row.of("1", new Timestamp(117, 11, 21, 0, 0, 0, 0), 8.06965530688617),
				Row.of("1", new Timestamp(117, 11, 22, 0, 0, 0, 0), 9.59076113897809),
				Row.of("1", new Timestamp(117, 11, 23, 0, 0, 0, 0), 8.51959031601596),
				Row.of("1", new Timestamp(117, 11, 24, 0, 0, 0, 0), 8.18367658262066),
				Row.of("1", new Timestamp(117, 11, 25, 0, 0, 0, 0), 8.07246736935477),
				Row.of("1", new Timestamp(117, 11, 26, 0, 0, 0, 0), 7.8935720735049),
				Row.of("1", new Timestamp(117, 11, 27, 0, 0, 0, 0), 7.78364059622125),
				Row.of("1", new Timestamp(117, 11, 28, 0, 0, 0, 0), 8.41405243249672),
				Row.of("1", new Timestamp(117, 11, 29, 0, 0, 0, 0), 8.82922635473185),
				Row.of("1", new Timestamp(117, 12, 1, 0, 0, 0, 0), 8.38251828808963),
				Row.of("1", new Timestamp(117, 12, 2, 0, 0, 0, 0), 8.06965530688617),
				Row.of("2", new Timestamp(117, 12, 3, 0, 0, 0, 0), 8.07246736935477),
				Row.of("2", new Timestamp(117, 12, 4, 0, 0, 0, 0), 7.8935720735049),
				Row.of("2", new Timestamp(117, 12, 5, 0, 0, 0, 0), 7.78364059622125),
				Row.of("2", new Timestamp(117, 12, 6, 0, 0, 0, 0), 8.41405243249672),
				Row.of("2", new Timestamp(117, 12, 7, 0, 0, 0, 0), 8.82922635473185),
				Row.of("2", new Timestamp(117, 12, 8, 0, 0, 0, 0), 8.38251828808963),
				Row.of("2", new Timestamp(117, 12, 9, 0, 0, 0, 0), 8.06965530688617)
			};
		String[] colNames = new String[] {"id", "ds1", "y1"};

		//train batch model.
		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(rowsData), colNames);

		ProphetTrainBatchOp model = new ProphetTrainBatchOp()
			.setTimeCol("ds1")
			.setValueCol("y1");

		source.link(model).print();

		//construct times series by id.
		GroupByBatchOp groupData = new GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("mtable_agg(ts, val) as data");

		ProphetPredictBatchOp prophetPredict = new ProphetPredictBatchOp()
			.setValueCol("ts")
			.setPredictNum(4)
			.setPredictionCol("pred");

		prophetPredict.linkFrom(model, source.link(groupData)).print();
	}
}
```

### 运行结果
id|data|predict
---|----|-------
1|{"data":{"ts":["1970-01-01 08:00:00.001","1970-01-01 08:00:00.002","1970-01-01 08:00:00.003","1970-01-01 08:00:00.004","1970-01-01 08:00:00.005","1970-01-01 08:00:00.006","1970-01-01 08:00:00.007","1970-01-01 08:00:00.008","1970-01-01 08:00:00.009","1970-01-01 08:00:00.01"],"val":[10.0,11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0]},"schema":"ts TIMESTAMP,val DOUBLE"}|{"data":{"ts":["1970-01-01 08:00:00.011","1970-01-01 08:00:00.012","1970-01-01 08:00:00.013","1970-01-01 08:00:00.014","1970-01-01 08:00:00.015","1970-01-01 08:00:00.016","1970-01-01 08:00:00.017","1970-01-01 08:00:00.018","1970-01-01 08:00:00.019","1970-01-01 08:00:00.02","1970-01-01 08:00:00.021","1970-01-01 08:00:00.022"],"val":[20.0,21.0,22.0,23.0,24.0,25.0,26.0,27.0,28.0,29.0,30.0,31.0]},"schema":"ts TIMESTAMP,val DOUBLE"}
t
