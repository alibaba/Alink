# 并行ID化预测 (HugeStringIndexerPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.HugeStringIndexerPredictBatchOp

Python 类名：HugeStringIndexerPredictBatchOp


## 功能介绍
提供字符串ID化处理功能，与 StringIndexerPredictBatchOp 功能相同，是其升级版本，模型为分布式存储，提升了运行效率。支持多列同时转换。

由 StringIndexerTrainBatchOp 生成词典模型，将输入数据的字符串转化成词典模型中的ID

对于词典模型中不存在的字符串，提供了三种处理策略，"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
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

model = stringindexer.linkFrom(data)

predictor = HugeStringIndexerPredictBatchOp()\
    .setSelectedCols(["f0", "f1"])\
    .setOutputCols(["f0_indexed", "f1_indexed"])

predictor.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeStringIndexerPredictBatchOpTest {
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
			.setStringOrderType("frequency_asc");
		BatchOperator <?> predictor = new HugeStringIndexerPredictBatchOp().setSelectedCols("f0", "f1")
			.setOutputCols("f0_indexed", "f1_indexed");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);
		BatchOperator result = predictor.linkFrom(model, data);
		result.print();
	}
}
```

### 运行结果
token|token_index
-----|-----------
banana|5
football|6
basketball|1
pingpang|2
tennis|3
pair|4
baseball|0
apple|7

f0|f1|f0_indexed|f1_indexed
---|---|----------|----------
basketball|apple|1|7
pingpang|banana|2|5
football|apple|6|7
tennis|pair|3|4
tennis|pair|3|4
basketball|apple|1|7
football|apple|6|7
football|apple|6|7
pingpang|banana|2|5
baseball|banana|0|5
