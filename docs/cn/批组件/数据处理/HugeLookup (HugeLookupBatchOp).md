# HugeLookup (HugeLookupBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.HugeLookupBatchOp

Python 类名：HugeLookupBatchOp


## 功能介绍
支持大数据量的查找功能，可实现两种数据按照某些列的合并操作，类似于数据的LEFT JOIN功能。

分别指定模型数据和输入数据中查找等值的Key列，模型数据中需要拼接到输入数据中的列名，最终输出数据数据列 + 模型数据拼接的列

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| mapKeyCols | Key列名 | 模型中对应的查找等值的列名 | String[] |  | null |
| mapValueCols | Values列名 | 模型中需要拼接到样本中的列名 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamUpdateMethod | 模型更新方法 | 模型更新方法，可选COMPLETE（全量更新）或者 INCREMENT（增量更新） | String |  | "COMPLETE" |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

data_df = pd.DataFrame([
    [10, 2.0], 
    [1, 2.0], 
    [-3, 2.0], 
    [5, 1.0]
])

inOp = BatchOperator.fromDataframe(data_df, schemaStr='f0 int, f1 double')

model_df = pd.DataFrame([
    [1, "value1"], 
    [2, "value2"], 
    [5, "value5"]
])

modelOp = BatchOperator.fromDataframe(model_df, schemaStr="key_col int, value_col string")

HugeLookupBatchOp()\
    .setMapKeyCols(["key_col"])\
    .setMapValueCols(["value_col"])\
    .setSelectedCols(["f0"])\
    .linkFrom(modelOp, inOp)\
    .print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.HugeLookupBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HugeLookupBatchOpTest {
	@Test
	public void testHugeLookupBatchOp() throws Exception {
		List <Row> data_df = Arrays.asList(
			Row.of(10, 2.0),
			Row.of(1, 2.0),
			Row.of(-3, 2.0),
			Row.of(5, 1.0)
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(data_df, "f0 int, f1 double");
		List <Row> model_df = Arrays.asList(
			Row.of(1, "value1"),
			Row.of(2, "value2"),
			Row.of(5, "value5")
		);
		BatchOperator <?> modelOp = new MemSourceBatchOp(model_df, "key_col int, value_col string");
		new HugeLookupBatchOp()
			.setMapKeyCols("key_col")
			.setMapValueCols("value_col")
			.setSelectedCols("f0")
			.linkFrom(modelOp, inOp)
			.print();
	}
}
```

### 运行结果
|f0|f1|value_col|
|---|---|---|
|10|2.0|null|
|1|2.0|value1|
|-3|2.0|null|
|5|1.0|value5|
