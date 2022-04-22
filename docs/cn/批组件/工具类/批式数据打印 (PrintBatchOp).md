# 批式数据打印 (PrintBatchOp)
Java 类名：com.alibaba.alink.operator.batch.utils.PrintBatchOp

Python 类名：PrintBatchOp


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
       ["0,0,0"],
       ["0.1,0.1,0.1"],
       ["0.2,0.2,0.2"],
       ["9,9,9"],
       ["9.1,9.1,9.1"],
       ["9.2,9.2,9.2"]
])

# batch source 
inOp = BatchOperator.fromDataframe(df, schemaStr='y string')

inOp.print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PrintBatchOpTest {
	@Test
	public void testPrintBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("0,0,0"),
			Row.of("0.1,0.1,0.1"),
			Row.of("0.2,0.2,0.2"),
			Row.of("9,9,9"),
			Row.of("9.1,9.1,9.1"),
			Row.of("9.2,9.2,9.2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "y string");
		inOp.print();
	}
}
```

### 运行结果

y|
|---|
0,0,0|
0.1,0.1,0.1|
0.2,0.2,0.2|
9,9,9|
9.1,9.1,9.1|
9.2,9.2,9.2|
