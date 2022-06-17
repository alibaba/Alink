# 加权采样 (WeightSampleBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.WeightSampleBatchOp

Python 类名：WeightSampleBatchOp


## 功能介绍
本算子是按照数据点的权重对数据按照比例进行加权采样，权重越大的数据点被采样的可能性越大。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| ratio | 采样比例 | 采样率，范围为[0, 1] | Double | ✓ | [0.0, 1.0] |  |
| weightCol | 权重列名 | 权重列对应的列名 | String | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| withReplacement | 是否放回 | 是否有放回的采样，默认不放回 | Boolean |  |  | false |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["a", 1.3, 1.1],
    ["b", 2.5, 0.9],
    ["c", 100.2, -0.01],
    ["d", 99.9, 100.9],
    ["e", 1.4, 1.1],
    ["f", 2.2, 0.9],
    ["g", 100.9, -0.01],
    ["j", 99.5, 100.9],
])



# batch source
inOp = BatchOperator.fromDataframe(df, schemaStr='id string, weight double, value double')
sampleOp = WeightSampleBatchOp() \
  .setWeightCol("weight") \
  .setRatio(0.5) \
  .setWithReplacement(False)

inOp.link(sampleOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.WeightSampleBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class WeightSampleBatchOpTest {
	@Test
	public void testWeightSampleBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 1.3, 1.1),
			Row.of("b", 2.5, 0.9),
			Row.of("c", 100.2, -0.01),
			Row.of("d", 99.9, 100.9),
			Row.of("e", 1.4, 1.1),
			Row.of("f", 2.2, 0.9),
			Row.of("g", 100.9, -0.01),
			Row.of("j", 99.5, 100.9)
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id string, weight double, value double");
		BatchOperator <?> sampleOp = new WeightSampleBatchOp()
			.setWeightCol("weight")
			.setRatio(0.5)
			.setWithReplacement(false);
		inOp.link(sampleOp).print();
	}
}
```
### 结果
id|weight|value
---|------|-----
g|100.9000|-0.0100
d|99.9000|100.9000
c|100.2000|-0.0100
j|99.5000|100.9000







