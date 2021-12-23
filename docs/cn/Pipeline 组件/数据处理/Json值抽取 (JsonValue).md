# Json值抽取 (JsonValue)
Java 类名：com.alibaba.alink.pipeline.dataproc.JsonValue

Python 类名：JsonValue


## 功能介绍

该组件完成json字符串中的信息抽取，按照用户给定的Path 抓取出相应的信息。该组件支持多Path抽取。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，必选 | String[] | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| jsonPath | Json 路径数组 | 用来指定 Json 抽取的内容。 | String[] | ✓ |  |
| outputColTypes | 输出结果列列类型数组 | 输出结果列类型数组，必选 | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| skipFailed | 是否跳过错误 | 当遇到抽取值为null 时是否跳过 | boolean |  | false |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["{a:boy,b:{b1:1,b2:2}}"],
    ["{a:girl,b:{b1:1,b2:2}}"]])

batchData = BatchOperator.fromDataframe(df, schemaStr='str string')

JsonValue()\
    .setJsonPath(["$.a","$.b.b1"])\
    .setSelectedCol("str")\
    .setOutputCols(["f0","f1"])\
    .transform(batchData)\
    .print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.JsonValue;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class JsonValueTest {
	@Test
	public void testJsonValue() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("{a:boy,b:{b1:1,b2:2}}")
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "str string");
		new JsonValue()
			.setJsonPath("$.a", "$.b.b1")
			.setSelectedCol("str")
			.setOutputCols("f0", "f1")
			.transform(batchData)
			.print();
	}
}
```

### 运行结果

str | f0 | f1
----|----|---
{a:boy,b:{b1:1,b2:2}}|boy|1
{a:girl,b:{b1:1,b2:2}}|girl|1



