# 超大ID化预测 (HugeIndexerStringPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.HugeIndexerStringPredictBatchOp

Python 类名：HugeIndexerStringPredictBatchOp


## 功能介绍
提供字符串ID化处理功能

由StringIndexerTrainBatchOp生成词典模型，将输入数据的ID类型转化成词典模型中对应的字符串。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ | 所选列类型为 [LONG] |  |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP", "ERROR", "SKIP" | "KEEP" |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
        ["football", "apple"],
   		["football", "apple"],
   		["football", "apple"],
   		["basketball", "apple"],
   		["basketball", "apple"],
   		["tennis", "pair"],
   		["tennis", "pair"],
   		["pingpang", "banana"],
   		["pingpang", "banana"],
   		["baseball", "banana"]
])

data = BatchOperator.fromDataframe(df, schemaStr='f0 string, f1 string')

stringindexer = StringIndexerTrainBatchOp()\
			.setSelectedCol("f0")\
			.setSelectedCols(["f1"])\
			.setStringOrderType("alphabet_asc")

predictor = HugeStringIndexerPredictBatchOp().setSelectedCols(["f0", "f1"])\
			.setOutputCols(["f0_indexed", "f1_indexed"])
		
model = stringindexer.linkFrom(data)
	
result = predictor.linkFrom(model, data)
		
indexerString = HugeIndexerStringPredictBatchOp().setSelectedCols(["f0_indexed", "f1_indexed"])\
			.setOutputCols(["f0_source", "f1_source"])
		
indexerString.linkFrom(model, result).print()
```
### Java 代码
```java
package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeIndexerStringPredictBatchOpTest {
	@Test
	public void testStringIndexerPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("basketball", "apple"),
			Row.of("basketball", "apple"),
			Row.of("tennis", "pair"),
			Row.of("tennis", "pair"),
			Row.of("pingpang", "banana"),
			Row.of("pingpang", "banana"),
			Row.of("baseball", "banana")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string,f1 string");

		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setSelectedCols("f1")
			.setStringOrderType("alphabet_asc");
		BatchOperator <?> predictor = new HugeStringIndexerPredictBatchOp().setSelectedCols("f0", "f1")
			.setOutputCols("f0_indexed", "f1_indexed");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);
		BatchOperator result = predictor.linkFrom(model, data);
		result.lazyPrint(10);

		BatchOperator <?> indexerString = new HugeIndexerStringPredictBatchOp().setSelectedCols("f0_indexed", "f1_indexed")
			.setOutputCols("f0_source", "f1_source");
		indexerString.linkFrom(model, result).print();
	}
}
```

### 运行结果
f0|f1|f0_indexed|f1_indexed|f0_source|f1_source
---|---|----------|----------|---------|---------
basketball|apple|3|0|basketball|apple
football|apple|4|0|football|apple
basketball|apple|3|0|basketball|apple
pingpang|banana|6|1|pingpang|banana
football|apple|4|0|football|apple
tennis|pair|7|5|tennis|pair
tennis|pair|7|5|tennis|pair
pingpang|banana|6|1|pingpang|banana
baseball|banana|2|1|baseball|banana
football|apple|4|0|football|apple
