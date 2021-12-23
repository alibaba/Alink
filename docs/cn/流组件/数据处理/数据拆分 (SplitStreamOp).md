# 数据拆分 (SplitStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.SplitStreamOp

Python 类名：SplitStreamOp


## 功能介绍
将数据集按比例拆分为两部分

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| fraction | 拆分到左端的数据比例 | 拆分到左端的数据比例 | Double | ✓ |  |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ['Ohio', 2000, 1.5],
    ['Ohio', 2001, 1.7],
    ['Ohio', 2002, 3.6],
    ['Nevada', 2001, 2.4],
    ['Nevada', 2002, 2.9],
    ['Nevada', 2003,3.2]
])

stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f1 string, f2 bigint, f3 double')

spliter = SplitStreamOp().setFraction(0.5)

spliter.linkFrom(stream_data).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.SplitStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SplitStreamOpTest {
	@Test
	public void testSplitStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("Ohio", 2000, 1.5),
			Row.of("Ohio", 2001, 1.7),
			Row.of("Ohio", 2002, 3.6),
			Row.of("Nevada", 2001, 2.4),
			Row.of("Nevada", 2002, 2.9),
			Row.of("Nevada", 2003, 3.2)
		);
		StreamOperator <?> stream_data = new MemSourceStreamOp(df_data, "f1 string, f2 int, f3 double");
		StreamOperator <?> spliter = new SplitStreamOp().setFraction(0.5);
		spliter.linkFrom(stream_data).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
f1|f2|f3
---|---|---
Nevada|2003|3.2000
Ohio|2000|1.5000
Ohio|2001|1.7000
Ohio|2002|3.6000
Nevada|2001|2.4000
Nevada|2002|2.9000
