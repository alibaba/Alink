# 固定条数分层随机采样 (StratifiedSampleWithSizeBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.StratifiedSampleWithSizeBatchOp

Python 类名：StratifiedSampleWithSizeBatchOp


## 功能介绍
固定条数分层随机采样组件。给定输入数据，本算法根据用户指定的不同类别的采样个数进行随机采样。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| strataCol | 分层列 | 分层列 | String | ✓ |  |  |
| strataSizes | 采样个数 | 采样个数, eg, a:10,b:30 | String | ✓ |  |  |
| strataSize | 采样个数 | 采样个数 | Integer |  |  | -1 |
| withReplacement | 是否放回 | 是否有放回的采样，默认不放回 | Boolean |  |  | false |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
        ['a',0.0,0.0],
        ['a',0.2,0.1],
        ['b',0.2,0.8],
        ['b',9.5,9.7],
        ['b',9.1,9.6],
        ['b',9.3,9.9]
    ])


batchData = BatchOperator.fromDataframe(df, schemaStr='x1 string, x2 double, x3 double')
sampleOp = StratifiedSampleWithSizeBatchOp() \
       .setStrataCol("x1") \
       .setStrataSizes("a:1,b:2")

batchData.link(sampleOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StratifiedSampleWithSizeBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StratifiedSampleWithSizeBatchOpTest {
	@Test
	public void testStratifiedSampleWithSizeBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 0.0, 0.0),
			Row.of("a", 0.2, 0.1),
			Row.of("b", 0.2, 0.8),
			Row.of("b", 9.5, 9.7),
			Row.of("b", 9.1, 9.6),
			Row.of("b", 9.3, 9.9)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "x1 string, x2 double, x3 double");
		BatchOperator <?> sampleOp = new StratifiedSampleWithSizeBatchOp()
			.setStrataCol("x1")
			.setStrataSizes("a:1,b:2");
		batchData.link(sampleOp).print();
	}
}
```

### 运行结果


x1|x2|x3
---|---|---
a|0.0000|0.0000
b|9.1000|9.6000
b|0.2000|0.8000
