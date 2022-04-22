# SQL操作：Where (WhereStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sql.WhereStreamOp

Python 类名：WhereStreamOp


## 功能介绍
对流式数据进行sql的WHERE操作。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |  |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["a", 1, 1.1, 1.2],
    ["b", -2, 0.9, 1.0],
    ["c", 100, -0.01, 1.0],
    ["d", -99, 100.9, 0.1],
    ["a", 1, 1.1, 1.2],
    ["b", -2, 0.9, 1.0],
    ["c", 100, -0.01, 0.2],
    ["d", -99, 100.9, 0.3]
])
source = StreamOperator.fromDataframe(df, schemaStr='col1 string, col2 int, col3 double, col4 double')
source.link(WhereStreamOp().setClause("col1='a'")).print()
StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class WhereStreamOpTest {

	@Test
	public void testWhereStreamOp() throws Exception {
		List <Row> inputRows = Arrays.asList(
			Row.of("a", 1, 1.1, 1.2),
			Row.of("b", -2, 0.9, 1.0),
			Row.of("c", 100, -0.01, 1.0),
			Row.of("d", -99, 100.9, 0.1),
			Row.of("a", 1, 1.1, 1.2),
			Row.of("b", -2, 0.9, 1.0),
			Row.of("c", 100, -0.01, 0.2),
			Row.of("d", -99, 100.9, 0.3)
		);
		StreamOperator <?> source = new MemSourceStreamOp(inputRows,
			"col1 string, col2 int, col3 double, col4 double");
		StreamOperator <?> output = source.link(new WhereStreamOp().setClause("col1='a'"));
		output.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

col1|col2|col3|col4
----|----|----|----
a|1|1.1000|1.2000
a|1|1.1000|1.2000
