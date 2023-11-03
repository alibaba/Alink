# ESD序列异常检测 (EsdOutlier4GroupedDataBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.EsdOutlier4GroupedDataBatchOp

Python 类名：EsdOutlier4GroupedDataBatchOp


## 功能介绍
ESD算法是一种常用的异常检测算法.
EsdOutlier4Series输入是MTable, 输出也是MTable, 返回序列数据(MTable)的所有异常点。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| inputMTableCol | 输入列名 | 输入序列的列名 | String | ✓ |  |  |
| outputMTableCol | 输出列名 | 输出序列的列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| alpha | 置信度 | 置信度 | Double |  |  | 0.05 |
| direction | 方向 | 检测异常的方向 | String |  | "POSITIVE", "NEGATIVE", "BOTH" | "BOTH" |
| featureCol | 特征列名 | 特征列名，默认选最左边的列 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| maxIter | 最大迭代步数 | 最大迭代步数 | Integer |  |  |  |
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

import pandas as pd
df = pd.DataFrame([
			[1, 1, 10.0],
			[1, 2, 11.0],
			[1, 3, 12.0],
			[1, 4, 13.0],
			[1, 5, 14.0],
			[1, 6, 15.0],
			[1, 7, 16.0],
			[1, 8, 17.0],
			[1, 9, 18.0],
			[1, 10, 19.0]
        ])

dataOp = BatchOperator.fromDataframe(df, schemaStr='group_id int, id int, val double')


outlierOp = dataOp.link(\
                GroupByBatchOp()\
                    .setGroupByPredicate("group_id")\
                    .setSelectClause("mtable_agg(id, val) as data")\
		).link(\
            EsdOutlier4GroupedDataBatchOp()\
                .setInputMTableCol("data")\
                .setOutputMTableCol("pred")\
                .setFeatureCol("val")\
                .setPredictionCol("detect_pred")\
		)

outlierOp.print()
```

### Java 代码

```java
package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EsdOutlier4SeriesBatchOpTest extends TestCase {
	@Test
	public void test() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, 1, 10.0),
			Row.of(1, 2, 11.0),
			Row.of(1, 3, 12.0),
			Row.of(1, 4, 13.0),
			Row.of(1, 5, 14.0),
			Row.of(1, 6, 15.0),
			Row.of(1, 7, 16.0),
			Row.of(1, 8, 17.0),
			Row.of(1, 9, 18.0),
			Row.of(1, 10, 19.0)
		);

		MemSourceBatchOp dataOp = new MemSourceBatchOp(mTableData, new String[] {"group_id", "id", "val"});

		BatchOperator <?> outlierOp = dataOp.link(
			new GroupByBatchOp()
				.setGroupByPredicate("group_id")
				.setSelectClause("mtable_agg(id, val) as data")
		).link(
			new EsdOutlier4GroupedDataBatchOp()
				.setInputMTableCol("data")
				.setOutputMTableCol("pred")
				.setFeatureCol("val")
				.setPredictionCol("detect_pred")
		);

		MTable pred = (MTable) outlierOp.collect().get(0).getField(0);
		Assert.assertEquals(0, pred.summary().sum("detect_pred"), 10e-10);

	}
}
```

### 运行结果

无


