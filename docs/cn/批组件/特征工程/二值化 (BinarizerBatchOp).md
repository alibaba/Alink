# 二值化 (BinarizerBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.BinarizerBatchOp

Python 类名：BinarizerBatchOp


## 功能介绍
给定一个阈值，将连续变量二值化。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| threshold | 二值化阈值 | 二值化阈值 | Double |  | 0.0 |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.1, True, "2", "A"],
    [1.1, False, "2", "B"],
    [1.1, True, "1", "B"],
    [2.2, True, "1", "A"]
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')

binarizer = BinarizerBatchOp().setSelectedCol("double").setThreshold(2.0)
binarizer.linkFrom(inOp1).print()

binarizer = BinarizerStreamOp().setSelectedCol("double").setThreshold(2.0)
binarizer.linkFrom(inOp2).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.BinarizerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.BinarizerStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BinarizerBatchOpTest {
	@Test
	public void testBinarizerBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.1, true, 2, "A"),
			Row.of(1.1, false, 2, "B"),
			Row.of(1.1, true, 1, "B"),
			Row.of(2.2, true, 1, "A")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "double double, bool boolean, number int, str string");
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "double double, bool boolean, number int, str string");
		BatchOperator <?> binarizer = new BinarizerBatchOp().setSelectedCol("double").setThreshold(2.0);
		binarizer.linkFrom(inOp1).print();
		StreamOperator <?> binarizer2 = new BinarizerStreamOp().setSelectedCol("double").setThreshold(2.0);
		binarizer2.linkFrom(inOp2).print();
		StreamOperator.execute();
	}
}
```
### 运行结果

##### 输出数据
double|bool|number|str
------|----|------|---
0.0000|true|2|A
0.0000|false|2|B
0.0000|true|1|B
1.0000|true|1|A
