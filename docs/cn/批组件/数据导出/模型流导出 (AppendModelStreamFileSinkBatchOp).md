# 模型流导出 (AppendModelStreamFileSinkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sink.AppendModelStreamFileSinkBatchOp

Python 类名：AppendModelStreamFileSinkBatchOp


## 功能介绍
将模型按照给定的时间戳，插入模型流。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| modelTime | 批模型时间戳 | 模型时间戳。默认当前时间。 使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |
| numFiles | 文件数目 | 文件数目 | Integer |  |  | 1 |
| numKeepModel | 保存模型的数目 | 实时写出模型的数目上限 | Integer |  |  | 2147483647 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.0, "A", 0, 0, 0, 1.0],
    [2.0, "B", 1, 1, 0, 2.0],
    [3.0, "C", 2, 2, 1, 3.0],
    [4.0, "D", 3, 3, 1, 4.0]
])

input = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 string, f2 int, f3 int, label int, reg_label double')

rfOp = RandomForestTrainBatchOp()\
    .setLabelCol("reg_label")\
    .setFeatureCols(["f0", "f1", "f2", "f3"])\
    .setFeatureSubsamplingRatio(0.5)\
    .setSubsamplingRatio(1.0)\
    .setNumTreesOfInfoGain(1)\
    .setNumTreesOfInfoGain(1)\
    .setNumTreesOfInfoGainRatio(1)\
    .setCategoricalCols(["f1"])

modelStream = AppendModelStreamFileSinkBatchOp()\
    .setFilePath("/tmp/random_forest_model_stream")\
    .setNumKeepModel(10)

rfOp.linkFrom(input).link(modelStream)

BatchOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AppendModelStreamFileSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;

public class AppendModelStreamFileSinkBatchOpTest {
	@Test
	public void testAppendModelStreamFileSinkBatchOp() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1.0, "A", 0L, 0, 0, 1.0),
			Row.of(2.0, "B", 1L, 1, 0, 2.0),
			Row.of(3.0, "C", 2L, 2, 1, 3.0),
			Row.of(4.0, "D", 3L, 3, 1, 4.0)
		};

		String[] colNames = new String[] {"f0", "f1", "f2", "f3", "label", "reg_label"};

		String labelColName = colNames[4];

		MemSourceBatchOp input = new MemSourceBatchOp(
			Arrays.asList(rows), new String[] {"f0", "f1", "f2", "f3", "label", "reg_label"}
		);

		RandomForestTrainBatchOp rfOp = new RandomForestTrainBatchOp()
			.setLabelCol(labelColName)
			.setFeatureCols(colNames[0], colNames[1], colNames[2], colNames[3])
			.setFeatureSubsamplingRatio(0.5)
			.setSubsamplingRatio(1.0)
			.setNumTreesOfInfoGain(1)
			.setNumTreesOfInfoGain(1)
			.setNumTreesOfInfoGainRatio(1)
			.setCategoricalCols(colNames[1]);

		rfOp.linkFrom(input).link(
			new AppendModelStreamFileSinkBatchOp()
				.setFilePath("/tmp/random_forest_model_stream")
				.setNumKeepModel(10)
		);

		BatchOperator.execute();
	}
}
```
