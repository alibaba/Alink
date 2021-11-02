# 随机采样 (SampleBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.SampleBatchOp

Python 类名：SampleBatchOp


## 功能介绍

- 本算子对数据进行随机抽样，每个样本都以相同的概率被抽到。



## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| ratio | 采样比例 | 采样率，范围为[0, 1] | Double | ✓ |  |
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

sampleOp = SampleBatchOp()\
        .setRatio(0.3)\
        .setWithReplacement(False)

inOp.link(sampleOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SampleBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SampleBatchOpTest {
	@Test
	public void testSampleBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("0,0,0"),
			Row.of("0.1,0.1,0.1"),
			Row.of("0.2,0.2,0.2"),
			Row.of("9,9,9"),
			Row.of("9.1,9.1,9.1"),
			Row.of("9.2,9.2,9.2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "Y string");
		BatchOperator <?> sampleOp = new SampleBatchOp()
			.setRatio(0.3)
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



