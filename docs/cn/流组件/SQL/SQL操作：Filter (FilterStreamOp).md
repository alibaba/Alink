# SQL操作：Filter (FilterStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sql.FilterStreamOp

Python 类名：FilterStreamOp


## 功能介绍
对流式数据进行sql的FILTER操作。

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
    ['Ohio', 2000, 1.5],
    ['Ohio', 2001, 1.7],
    ['Ohio', 2002, 3.6],
    ['Nevada', 2001, 2.4],
    ['Nevada', 2002, 2.9],
    ['Nevada', 2003, 3.2]
])

stream_data = StreamOperator.fromDataframe(df, schemaStr='f1 string, f2 bigint, f3 double')

op = FilterStreamOp().setClause("f1='Ohio'")
stream_data = stream_data.link(op)

stream_data.print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.FilterStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FilterStreamOpTest {
	@Test
	public void testFilterStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		StreamOperator <?> stream_data = new MemSourceStreamOp(df, "f1 string, f2 int, f3 double");
		StreamOperator <?> op = new FilterStreamOp().setClause("f1='Ohio'");
		stream_data = stream_data.link(op);
		stream_data.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

f1|f2|f3
---|---|---
Ohio|2000|1.5000
Ohio|2001|1.7000
Ohio|2002|3.6000
