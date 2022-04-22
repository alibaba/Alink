# 打乱数据顺序 (ShuffleBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp

Python 类名：ShuffleBatchOp


## 功能介绍
将输入数据的顺序打乱。

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
     
inOp = BatchOperator.fromDataframe(df, schemaStr='Y string')

inOp.link(ShuffleBatchOp()).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ShuffleBatchOpTest {
	@Test
	public void testShuffleBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("0,0,0"),
			Row.of("0.1,0.1,0.1"),
			Row.of("0.2,0.2,0.2"),
			Row.of("9,9,9"),
			Row.of("9.1,9.1,9.1"),
			Row.of("9.2,9.2,9.2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "Y string");
		inOp.link(new ShuffleBatchOp()).print();
	}
}
```

### 运行结果

Y
---
0.2,0.2,0.2
9.2,9.2,9.2
9,9,9
9.1,9.1,9.1
0,0,0
0.1,0.1,0.1
