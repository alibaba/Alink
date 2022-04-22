# JSON值抽取 (JsonValueStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp

Python 类名：JsonValueStreamOp


## 功能介绍

该组件完成json字符串中的信息抽取，按照用户给定的Path 抓取出相应的信息。该组件支持
- 按照多JsonPath规则编写的多条抽取规则。
- 默认输出类型为string，也可以根据实际情况指定数据类型

### JsonPath表达式

| 元素 | 含义 |
--- | ---
| $ | 表示文档的根元素 |
| @ | 表示文档的当前元素 |
| .node_name 或 ['node_name'] | 匹配下级节点 |
| [index] | 选择数组中的元素 |
| [start:end:step] | 表示数组切片语法 |

- 注意事项：jsonPath参数是字符串数组，长度应当和指定的输出列名称数目保持一致。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| jsonPath | Json 路径数组 | 用来指定 Json 抽取的内容。 | String[] | ✓ |  |  |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，必选 | String[] | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| outputColTypes | 输出结果列列类型数组 | 输出结果列类型数组 | String[] |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| skipFailed | 是否跳过错误 | 当遇到抽取值为null 时是否跳过 | boolean |  |  | false |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
     ["{a:boy,b:{b1:[1,2],b2:2}}"],
     ["{a:girl,b:{b1:[1,3],b2:2}}"]
 ])
 
streamData = StreamOperator.fromDataframe(df, schemaStr='str string')
 
JsonValueStreamOp()\
     .setJsonPath(["$.a", "$.b.b1[0]","$.b.b2"])\
     .setSelectedCol("str")\
     .setOutputCols(["f0","f1","f2"])\
     .setOutputColTypes(["string", "int", "long"])\
     .linkFrom(streamData)\
     .print()
     
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class JsonValueStreamOpTest {
	@Test
	public void testJsonValueStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("{a:boy,b:{b1:[1,2],b2:2}}"),
			Row.of("{a:girl,b:{b1:[1,3],b2:2}}")
		);
		StreamOperator <?> streamData = new MemSourceStreamOp(df, "str string");
		new JsonValueStreamOp()
			.setJsonPath("$.a", "$.b.b1[0]","$.b.b2")
			.setSelectedCol("str")
			.setOutputCols("f0", "f1", "f2")
			.setOutputColTypes("string", "int", "long")
			.linkFrom(streamData)
			.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

str|f0|f1|f2
---|---|---|---
{a:girl,b:{b1:[1,3],b2:2}}|girl|1|2
{a:boy,b:{b1:[1,2],b2:2}}|boy|1|2





