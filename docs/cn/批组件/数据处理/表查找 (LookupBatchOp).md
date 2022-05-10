# 表查找 (LookupBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.LookupBatchOp

Python 类名：LookupBatchOp


## 功能介绍
支持数据查找功能，支持多个key的查找，并将查找后的结果中的value列添加到待查询数据后面。与SQl语法中的inner join功能类似，当不存在重复的key时
LookupBatchOp().setMapKeyCols("key_col_A").setMapValueCols("value_col").setSelectedCols("key_col_B").linkFrom(A, B)与
"SELECT A.value_col FROM A INNER JOIN B ON A.key_col_A = B.key_col_B"，但是需要注意：当数据B中存在多行相同的key时，只保留一个value，不会找到所有的value。

 Table A
 
| key_col_A | value_col |
--- | ---
| Bob | 98 |
| Tom | 72 |

 Table B
 
| key_col_B | age |
--- | ---
| Bob | 11 |
| Denny | 10 |

查找结果

| key_col_B | age |value_col |
--- | --- | ---
| Bob | 11 | 98
| Denny | 10 | null

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| mapKeyCols | Key列名 | 模型中对应的查找等值的列名 | String[] |  |  | null |
| mapValueCols | Values列名 | 模型中需要拼接到样本中的列名 | String[] |  |  | null |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamUpdateMethod | 模型更新方法 | 模型更新方法，可选COMPLETE（全量更新）或者 INCREMENT（增量更新） | String |  | "COMPLETE", "INCREMENT" | "COMPLETE" |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["10", 2.0], ["1", 2.0], ["-3", 2.0], ["5", 1.0]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='f0 string, f1 double')

df2 = pd.DataFrame([
    ["1", "value1"], ["2", "value2"], ["5", "value5"]
])
modelOp = BatchOperator.fromDataframe(df2, schemaStr="key_col string, value_col string")

LookupBatchOp().setMapKeyCols(["key_col"]).setMapValueCols(["value_col"]) \
    .setSelectedCols(["f0"]).linkFrom(modelOp, inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.LookupBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LookupBatchOpTest {
	@Test
	public void testLookupBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("10", 2.0), Row.of("1", 2.0), Row.of("-3", 2.0), Row.of("5", 1.0)
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "f0 string, f1 double");
		List <Row> df2 = Arrays.asList(
			Row.of("1", "value1"), Row.of("2", "value2"), Row.of("5", "value5")
		);
		BatchOperator <?> modelOp = new MemSourceBatchOp(df2, "key_col string, value_col string");
		new LookupBatchOp().setMapKeyCols("key_col").setMapValueCols("value_col")
			.setSelectedCols("f0").linkFrom(modelOp, inOp).print();
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
