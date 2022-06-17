# 分层随机采样 (StratifiedSampleStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.StratifiedSampleStreamOp

Python 类名：StratifiedSampleStreamOp


## 功能介绍
分层采样组件。给定输入数据，本算法根据用户指定的不同类别的采样比例进行随机采样。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| strataCol | 分层列 | 分层列 | String | ✓ |  |  |
| strataRatios | 采用比率 | 采用比率, eg, a:0.1,b:0.3 | String | ✓ |  |  |
| strataRatio | 采用比率 | 采用比率 | Double |  |  | -1.0 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
        ['a',0.0,0.0],
        ['a',0.2,0.1],
        ['b',0.2,0.8],
        ['b',9.5,9.7],
        ['b',9.1,9.6],
        ['b',9.3,9.9]
    ])

streamData = StreamOperator.fromDataframe(df_data, schemaStr='x1 string, x2 double, x3 double')
sampleStreamOp = StratifiedSampleStreamOp()\
       .setStrataCol("x1")\
       .setStrataRatios("a:0.5,b:0.5")

sampleStreamOp.linkFrom(streamData).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.StratifiedSampleStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StratifiedSampleStreamOpTest {
	@Test
	public void testStratifiedSampleStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("a", 0.0, 0.0),
			Row.of("a", 0.2, 0.1),
			Row.of("b", 0.2, 0.8),
			Row.of("b", 9.5, 9.7),
			Row.of("b", 9.1, 9.6),
			Row.of("b", 9.3, 9.9)
		);
		StreamOperator <?> streamData = new MemSourceStreamOp(df_data, "x1 string, x2 double, x3 double");
		StreamOperator <?> sampleStreamOp = new StratifiedSampleStreamOp()
			.setStrataCol("x1")
			.setStrataRatios("a:0.5,b:0.5");
		sampleStreamOp.linkFrom(streamData).print();
		StreamOperator.execute();
	}
}
```
### 运行结果

x1|x2|x3
---|---|---
b|9.3|9.9
a|0.0|0.0
b|0.2|0.8


