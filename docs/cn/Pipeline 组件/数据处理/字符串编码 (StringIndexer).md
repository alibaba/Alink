# 字符串编码 (StringIndexer)
Java 类名：com.alibaba.alink.pipeline.dataproc.StringIndexer

Python 类名：StringIndexer


## 功能介绍
StringIndexer训练组件的作用是训练一个模型用于将单列字符串映射为整数。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP", "ERROR", "SKIP" | "KEEP" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| modelName | 模型名字 | 模型名字 | String |  |  |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |
| stringOrderType | Token排序方法 | Token排序方法 | String |  | "RANDOM", "FREQUENCY_ASC", "FREQUENCY_DESC", "ALPHABET_ASC", "ALPHABET_DESC" | "RANDOM" |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ["football"],
    ["football"],
    ["football"],
    ["basketball"],
    ["basketball"],
    ["tennis"],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='f0 string')

stringindexer = StringIndexer() \
    .setSelectedCol("f0") \
    .setOutputCol("f0_indexed") \
    .setStringOrderType("frequency_asc")

stringindexer.fit(data).transform(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.StringIndexer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StringIndexerTest {
	@Test
	public void testStringIndexer() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("football"),
			Row.of("football"),
			Row.of("football"),
			Row.of("basketball"),
			Row.of("basketball"),
			Row.of("tennis")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "f0 string");
		StringIndexer stringindexer = new StringIndexer()
			.setSelectedCol("f0")
			.setOutputCol("f0_indexed")
			.setStringOrderType("frequency_asc");
		stringindexer.fit(data).transform(data).print();
	}
}
```

### 运行结果

f0|f0_indexed
---|----------
football|2
football|2
football|2
basketball|1
basketball|1
tennis|0
