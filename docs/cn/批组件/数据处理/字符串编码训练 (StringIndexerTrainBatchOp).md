# 字符串编码训练 (StringIndexerTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp

Python 类名：StringIndexerTrainBatchOp


## 功能介绍
StringIndexer训练组件的作用是训练一个模型用于将单列字符串映射为整数。

如将一列映射为整数，需指定 setSelectedCol 设定。

同时，该组件支持输入多列，生成一个映射词典，通过 setSelectedCols 设定其他需要补充的列名。

特征的排列顺序支持 random，frequency_asc，frequency_desc，alphabet_asc，alphabet_desc 五种排序方法。

注意：输入多列时，所有列必须为相同格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| modelName | 模型名字 | 模型名字 | String |  |  |  |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | 所选列类型为 [INTEGER, LONG, STRING] | null |
| stringOrderType | Token排序方法 | Token排序方法 | String |  | "RANDOM", "FREQUENCY_ASC", "FREQUENCY_DESC", "ALPHABET_ASC", "ALPHABET_DESC" | "RANDOM" |



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


data = BatchOperator.fromDataframe(df, schemaStr='f0 string,f1 string')

stringindexer = StringIndexerTrainBatchOp() \
    .setSelectedCol("f0") \
    .setSelectedCols(["f1"]) \
    .setStringOrderType("alphabet_asc")

model = stringindexer.linkFrom(data)
model.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StringIndexerTrainBatchOpTest {
	@Test
	public void testAlphabetAsc() throws Exception {
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
		BatchOperator model = stringindexer.linkFrom(data);
		model.print();
	}
}
```

### 运行结果

模型表：

token|token_index
-----|-----------
pingpang|6
banana|1
baseball|2
basketball|3
pair|5
apple|0
football|4
tennis|7


