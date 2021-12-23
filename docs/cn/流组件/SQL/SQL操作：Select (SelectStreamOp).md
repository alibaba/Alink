# SQL操作：Select (SelectStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sql.SelectStreamOp

Python 类名：SelectStreamOp


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |

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

op = SelectStreamOp().setClause('f1 as river')
stream_data = stream_data.link(op)

stream_data.print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.SelectStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SelectStreamOpTest {
	@Test
	public void testSelectStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		StreamOperator <?> stream_data = new MemSourceStreamOp(df, "f1 string, f2 int, f3 double");
		StreamOperator <?> op = new SelectStreamOp().setClause("f1 as river");
		stream_data = stream_data.link(op);
		stream_data.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

river|
-----|
Nevada|
Nevada|
Ohio|
Nevada|
Ohio|
Ohio|
