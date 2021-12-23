# 分位数离散化 (QuantileDiscretizer)
Java 类名：com.alibaba.alink.pipeline.feature.QuantileDiscretizer

Python 类名：QuantileDiscretizer


## 功能介绍

分位点离散可以计算选定列的分位点，然后使用这些分位点进行离散化。
生成选中列对应的q-quantile，其中可以所有列指定一个，也可以每一列对应一个

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| dropLast | 是否删除最后一个元素 | 删除最后一个元素是为了保证线性无关性。默认true | Boolean |  | true |
| encode | 编码方法 | 编码方法 | String |  | "INDEX" |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP" |
| leftOpen | 是否左开右闭 | 左开右闭为true，左闭右开为false | Boolean |  | true |
| numBuckets | quantile个数 | quantile个数，对所有列有效。 | Integer |  | 2 |
| numBucketsArray | quantile个数 | quantile个数，每一列对应数组中一个元素。 | Integer[] |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["a", 1, 1, 2.0, True],
    ["c", 1, 2, -3.0, True],
    ["a", 2, 2, 2.0, False],
    ["c", 0, 0, 0.0, False]
])

batchSource = BatchOperator.fromDataframe(
    df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')
streamSource = StreamOperator.fromDataframe(
    df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')

QuantileDiscretizer()\
    .setSelectedCols(['f_double'])\
    .setNumBuckets(8)\
    .fit(batchSource)\
    .transform(batchSource)\
    .print()

QuantileDiscretizer()\
    .setSelectedCols(['f_double'])\
    .setNumBuckets(8)\
    .fit(batchSource)\
    .transform(streamSource)\
    .print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.feature.QuantileDiscretizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class QuantileDiscretizerTest {
	@Test
	public void testQuantileDiscretizer() throws Exception {
		List <Row> sourceFrame = Arrays.asList(
			Row.of("a", 1, 1, 2.0, true),
			Row.of("c", 1, 2, -3.0, true),
			Row.of("a", 2, 2, 2.0, false),
			Row.of("c", 0, 0, 0.0, false)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(sourceFrame,
			"f_string string, f_long int, f_int int, f_double double, f_boolean boolean");
		StreamOperator <?> streamSource = new MemSourceStreamOp(sourceFrame,
			"f_string string, f_long int, f_int int, f_double double, f_boolean boolean");
		new QuantileDiscretizer().setSelectedCols("f_double").setNumBuckets(8).fit(batchSource).transform(batchSource)
			.print();
		new QuantileDiscretizer().setSelectedCols("f_double").setNumBuckets(8).fit(batchSource).transform(streamSource)
			.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
f_string|f_long|f_int|f_double|f_boolean
--------|------|-----|--------|---------
a|1|1|2|true
c|1|2|0|true
a|2|2|2|false
c|0|0|1|false
