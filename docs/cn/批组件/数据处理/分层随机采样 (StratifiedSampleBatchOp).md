# 分层随机采样 (StratifiedSampleBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.StratifiedSampleBatchOp

Python 类名：StratifiedSampleBatchOp


## 功能介绍

本算子是对每个类别按照比例进行分层随机抽样。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| strataCol | 分层列 | 分层列 | String | ✓ |  |
| strataRatios | 采用比率 | 采用比率, eg, a:0.1,b:0.3 | String | ✓ |  |
| withReplacement | 是否放回 | 是否有放回的采样，默认不放回 | Boolean |  | false |
| strataRatio | 采用比率 | 采用比率 | Double |  | -1.0 |


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
sampleOp = StratifiedSampleBatchOp()\
       .setStrataCol("x1")\
       .setStrataRatios("a:0.5,b:0.5")

batchData.link(sampleOp).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StratifiedSampleBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StratifiedSampleBatchOpTest {
	@Test
	public void testStratifiedSampleBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 0.0, 0.0),
			Row.of("a", 0.2, 0.1),
			Row.of("b", 0.2, 0.8),
			Row.of("b", 9.5, 9.7),
			Row.of("b", 9.1, 9.6),
			Row.of("b", 9.3, 9.9)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "x1 string, x2 double, x3 double");
		BatchOperator <?> sampleOp = new StratifiedSampleBatchOp()
			.setStrataCol("x1")
			.setStrataRatios("a:0.5,b:0.5");
		batchData.link(sampleOp).print();
	}
}
```

### 运行结果


x1|x2|x3
---|---|---
a|0.0000|0.0000
b|9.5000|9.7000
b|9.1000|9.6000
b|9.3000|9.9000
