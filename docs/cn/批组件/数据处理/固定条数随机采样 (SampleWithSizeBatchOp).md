# 固定条数随机采样 (SampleWithSizeBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.SampleWithSizeBatchOp

Python 类名：SampleWithSizeBatchOp


## 功能介绍
对数据按个数进行随机抽样，每个样本都以相同的概率被抽到。


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| size | 采样个数 | 采样个数 | Integer | ✓ |  |
| withReplacement | 是否放回 | 是否有放回的采样，默认不放回 | Boolean |  | false |


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

sampleOp = SampleWithSizeBatchOp() \
  .setSize(2) \
  .setWithReplacement(False)

inOp.link(sampleOp).print()


```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SampleWithSizeBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SampleWithSizeBatchOpTest {
	@Test
	public void testSampleWithSizeBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("0,0,0"),
			Row.of("0.1,0.1,0.1"),
			Row.of("0.2,0.2,0.2"),
			Row.of("9,9,9"),
			Row.of("9.1,9.1,9.1"),
			Row.of("9.2,9.2,9.2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "Y string");
		BatchOperator <?> sampleOp = new SampleWithSizeBatchOp()
			.setSize(2)
			.setWithReplacement(false);
		inOp.link(sampleOp).print();
	}
}
```
### 运行结果

|Y|
|---|
|0,0,0|
|0.2,0.2,0.2|




