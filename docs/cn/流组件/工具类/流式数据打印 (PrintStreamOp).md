# 流式数据打印 (PrintStreamOp)
Java 类名：com.alibaba.alink.operator.stream.utils.PrintStreamOp

Python 类名：PrintStreamOp


## 功能介绍
该组件打印表中数据。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |




## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, "abcde", "aabce"],
    [1, "aacedw", "aabbed"],
    [2, "cdefa", "bbcefa"],
    [3, "bdefh", "ddeac"],
    [4, "acedm", "aeefbc"]
])

inOp = StreamOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')

inOp.print()

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

public class PrintStreamOpTest {
	@Test
	public void testPrintStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "abcde", "aabce"),
			Row.of(1, "aacedw", "aabbed"),
			Row.of(2, "cdefa", "bbcefa"),
			Row.of(3, "bdefh", "ddeac"),
			Row.of(4, "acedm", "aeefbc")
		);
		StreamOperator <?> inOp = new MemSourceStreamOp(df, "id int, text1 string, text2 string");
		inOp.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

id|text1|text2
---|-----|-----
4|acedm|aeefbc
2|cdefa|bbcefa
3|bdefh|ddeac
1|aacedw|aabbed
0|abcde|aabce
