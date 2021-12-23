# 多列并行反ID化预测 (HugeMultiIndexerStringPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.HugeMultiIndexerStringPredictBatchOp

Python 类名：HugeMultiIndexerStringPredictBatchOp


## 功能介绍
提供ID转换为字符串的功能，与 HugeMultiStringIndexerPredictBatchOp 功能相反。

由 MultiStringIndexerTrainBatchOp 生成词典模型，将输入数据的ID转化成原文。

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
    [1, "football", "apple"],
    [2, "football", "apple"],
    [3, "football", "apple"],
    [4, "basketball", "apple"],
    [5, "basketball", "apple"],
    [6, "tennis", "pair"],
    [7, "tennis", "pair"],
    [8, "pingpang", "banana"],
    [9, "pingpang", "banana"],
    [0, "baseball", "banana"]
])

data = BatchOperator.fromDataframe(df, schemaStr='id long, f0 string, f1 string')

stringindexer = MultiStringIndexerTrainBatchOp()\
    .setSelectedCols(["f0", "f1"])\
    .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)

predictor = HugeMultiStringIndexerPredictBatchOp()\
    .setSelectedCols(["f0", "f1"])
result = predictor.linkFrom(model, data)

stringPredictor = HugeMultiIndexerStringPredictBatchOp()\
    .setSelectedCols(["f0", "f1"])\
    .setOutputCols(["f0_source", "f1_source"])
stringPredictor.linkFrom(model, result).print();
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeMultiIndexerStringPredictBatchOpTest {
	@Test
	public void testHugeMultiStringIndexerPredict() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1L, "football", "apple"),
			Row.of(2L, "football", "apple"),
			Row.of(3L, "football", "apple"),
			Row.of(4L, "basketball", "apple"),
			Row.of(5L, "basketball", "apple"),
			Row.of(6L, "tennis", "pair"),
			Row.of(7L, "tennis", "pair"),
			Row.of(8L, "pingpang", "banana"),
			Row.of(9L, "pingpang", "banana"),
			Row.of(0L, "baseball", "banana")
		);
		// baseball 1
		// basketball,pair,tennis,pingpang 2
		// footbal,banana 3
		// apple 5
		BatchOperator <?> data = new MemSourceBatchOp(df, "id long,f0 string,f1 string");
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.setStringOrderType("frequency_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);

		BatchOperator <?> predictor = new HugeMultiStringIndexerPredictBatchOp().setSelectedCols("f0", "f1");
		BatchOperator result = predictor.linkFrom(model, data);
		result.lazyPrint(10);

		BatchOperator <?> stringPredictor = new HugeMultiIndexerStringPredictBatchOp().setSelectedCols("f0", "f1")
			.setOutputCols("f0_source", "f1_source");
		stringPredictor.linkFrom(model, result).print();
	}
}
```

### 运行结果
column_index|token|token_index
------------|-----|-----------
1|apple|2
1|pair|0
1|banana|1
-1|{"selectedCols":"[\"f0\",\"f1\"]","selectedColTypes":"[\"VARCHAR\",\"VARCHAR\"]"}|null
0|football|4
0|baseball|0
0|basketball|1
0|tennis|2
0|pingpang|3

id|f0|f1
---|---|---
1|4|2
2|4|2
6|2|0
7|2|0
5|1|2
3|4|2
9|3|1
0|0|1
4|1|2
8|3|1

id|f0|f1|f0_source|f1_source
---|---|---|---------|---------
5|1|2|basketball|apple
2|4|2|football|apple
6|2|0|tennis|pair
4|1|2|basketball|apple
8|3|1|pingpang|banana
3|4|2|football|apple
7|2|0|tennis|pair
9|3|1|pingpang|banana
0|0|1|baseball|banana
1|4|2|football|apple
