# 前N个数 (FirstNBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.FirstNBatchOp

Python 类名：FirstNBatchOp


## 功能介绍
该组件输出表的前N条数据。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| size | 采样个数 | 采样个数 | Integer | ✓ | [1, +inf) |  |



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
inOp = BatchOperator.fromDataframe(df, schemaStr='Y string')

sampleOp = FirstNBatchOp()\
        .setSize(2)

inOp.link(sampleOp).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.FirstNBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FirstNBatchOpTest {
	@Test
	public void testFirstNBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("0,0,0"),
			Row.of("0.1,0.1,0.1"),
			Row.of("0.2,0.2,0.2"),
			Row.of("9,9,9"),
			Row.of("9.1,9.1,9.1"),
			Row.of("9.2,9.2,9.2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "Y string");
		BatchOperator <?> sampleOp = new FirstNBatchOp()
			.setSize(2);
		inOp.link(sampleOp).print();
	}
}
```

### 运行结果
|Y|
|---|
|0,0,0|
|0.1,0.1,0.1|
