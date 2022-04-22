# 分桶 (Bucketizer)
Java 类名：com.alibaba.alink.pipeline.feature.Bucketizer

Python 类名：Bucketizer


## 功能介绍
给定切分点，将连续变量分桶，需要选择需要进行切分的单列或多列，同时给出选中每列的切分点，每列切分点都是一个double数组，需要严格递增。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| cutsArray | 多列的切分点 | 多列的切分点 | double[][] |  |  |  |
| dropLast | 是否删除最后一个元素 | 删除最后一个元素是为了保证线性无关性。默认true | Boolean |  |  | true |
| encode | 编码方法 | 编码方法 | String |  | "VECTOR", "ASSEMBLED_VECTOR", "INDEX" | "INDEX" |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP", "ERROR", "SKIP" | "KEEP" |
| leftOpen | 是否左开右闭 | 左开右闭为true，左闭右开为false | Boolean |  |  | true |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


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

inOp = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
bucketizer = Bucketizer().setSelectedCols(["double"]).setCutsArray([[2.0]])
bucketizer.transform(inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.feature.Bucketizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BucketizerTest {
	@Test
	public void testBucketizer() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.1, true, 2, "A"),
			Row.of(1.1, false, 2, "B"),
			Row.of(1.1, true, 1, "B"),
			Row.of(2.2, true, 1, "A")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "double double, bool boolean, number int, str string");
		double[] cutsArray = {2.0};
		Bucketizer bucketizer = new Bucketizer().setSelectedCols("double").setCutsArray(cutsArray);
		bucketizer.transform(inOp).print();
	}
}
```
### 运行结果

double|bool|number|str
------|----|------|---
0|true|2|A
0|false|2|B
0|true|1|B
1|true|1|A
