# HugeStringIndexer预测 (HugeMultiStringIndexerPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.HugeMultiStringIndexerPredictBatchOp

Python 类名：HugeMultiStringIndexerPredictBatchOp


## 功能介绍
提供字符串ID化处理功能

由MultiStringIndexerTrainBatchOp生成词典模型，将输入数据的字符串转化成词典模型中的ID

对于词典模型中不存在的字符串，提供了三种处理策略，"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP" |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["a", 1], ["b", 2], ["b", 3], ["c", 4]
])

op = BatchOperator.fromDataframe(df, schemaStr='f0 string, f1 int')

stringIndexer = MultiStringIndexerTrainBatchOp().setSelectedCols(["f1", "f0"]).setStringOrderType("frequency_desc")
stringIndexer.linkFrom(op)

predictor = HugeMultiStringIndexerPredictBatchOp().setSelectedCols(["f0"]).setReservedCols(["f0", "f1"])\
    .setOutputCols(["f0_index"]).setHandleInvalid("skip");
predictor.linkFrom(stringIndexer, op).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.HugeMultiStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeMultiStringIndexerPredictBatchOpTest {
	@Test
	public void testHugeMultiStringIndexerPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 1), Row.of("b", 2), Row.of("b", 3), Row.of("c", 4)
		);
		BatchOperator <?> op = new MemSourceBatchOp(df, "f0 string, f1 int");
		BatchOperator <?> stringIndexer = new MultiStringIndexerTrainBatchOp().setSelectedCols("f1", "f0")
			.setStringOrderType("frequency_desc");
		stringIndexer.linkFrom(op);
		BatchOperator <?> predictor = new HugeMultiStringIndexerPredictBatchOp().setSelectedCols("f0").setReservedCols(
			"f0", "f1")
			.setOutputCols("f0_index").setHandleInvalid("skip");
		predictor.linkFrom(stringIndexer, op).print();
	}
}
```

### 运行结果
|f0|f1|f0_index|
|---|---|---|
|a|1|1|
|b|2|0|
|b|3|0|
|c|4|2|
