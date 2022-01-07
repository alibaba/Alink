# 分桶 (BucketizerStreamOp)
Java 类名：com.alibaba.alink.operator.stream.feature.BucketizerStreamOp

Python 类名：BucketizerStreamOp


## 功能介绍
给定切分点，将连续变量分桶，可支持单列输入或多列输入，对应需要给出单列切分点或者多列切分点。

每列切分点需要严格递增，且至少有三个点。


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| cutsArray | 多列的切分点 | 多列的切分点 | double[][] |  |  |
| dropLast | 是否删除最后一个元素 | 删除最后一个元素是为了保证线性无关性。默认true | Boolean |  | true |
| encode | 编码方法 | 编码方法 | String |  | "INDEX" |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP" |
| leftOpen | 是否左开右闭 | 左开右闭为true，左闭右开为false | Boolean |  | true |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
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

bucketizer = BucketizerBatchOp().setSelectedCols(["double"]).setCutsArray([[2.0]])
bucketizer.linkFrom(inOp1).print()

bucketizer = BucketizerStreamOp().setSelectedCols(["double"]).setCutsArray([[2.0]])
bucketizer.linkFrom(inOp2).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.BucketizerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.BucketizerStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BucketizerStreamOpTest {
	@Test
	public void testBucketizerStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.1, true, 2, "A"),
			Row.of(1.1, false, 2, "B"),
			Row.of(1.1, true, 1, "B"),
			Row.of(2.2, true, 1, "A")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "double double, bool boolean, number int, str string");
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "double double, bool boolean, number int, str string");
		double[] cutsArr = {2.0};
		BatchOperator <?> bucketizer = new BucketizerBatchOp().setSelectedCols("double").setCutsArray(cutsArr);
		bucketizer.linkFrom(inOp1).print();
		StreamOperator <?> bucketizer2 = new BucketizerStreamOp().setSelectedCols("double").setCutsArray(cutsArr);
		bucketizer2.linkFrom(inOp2).print();
		StreamOperator.execute();
	}
}
```
### 运行结果
批预测结果

double|bool|number|str
------|----|------|---
0|true|2|A
0|false|2|B
0|true|1|B
1|true|1|A

流预测结果

double|bool|number|str
------|----|------|---
0|true|2|A
0|true|1|B
0|false|2|B
1|true|1|A
