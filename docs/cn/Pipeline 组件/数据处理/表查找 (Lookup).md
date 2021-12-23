# 表查找 (Lookup)
Java 类名：com.alibaba.alink.pipeline.dataproc.Lookup

Python 类名：Lookup


## 功能介绍
支持数据查找功能，支持多个key的查找，并将查找后的结果中的value列添加到待查询数据后面。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| mapKeyCols | Key列名 | 模型中对应的查找等值的列名 | String[] |  | null |
| mapValueCols | Values列名 | 模型中需要拼接到样本中的列名 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamUpdateMethod | 模型更新方法 | 模型更新方法，可选COMPLETE（全量更新）或者 INCREMENT（增量更新） | String |  | "COMPLETE" |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

data_df = pd.DataFrame([
    ["10", 2.0], 
    ["1", 2.0], 
    ["-3", 2.0], 
    ["5", 1.0]
])

inOp = StreamOperator.fromDataframe(data_df, schemaStr='f0 string, f1 double')

data_df = pd.DataFrame([
    ["1", "value1"], 
    ["2", "value2"], 
    ["5", "value5"]
])

modelOp = BatchOperator.fromDataframe(data_df, schemaStr="key_col string, value_col string")

Lookup()\
    .setModelData(modelOp)\
    .setMapKeyCols(["key_col"])\
    .setMapValueCols(["value_col"]) \
    .setSelectedCols(["f0"])\
    .transform(inOp)\
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
import com.alibaba.alink.pipeline.dataproc.Lookup;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LookupTest {
	@Test
	public void testLookup() throws Exception {
		List <Row> data_df = Arrays.asList(
			Row.of("10", 2.0),
			Row.of("1", 2.0),
			Row.of("-3", 2.0),
			Row.of("5", 1.0)
		);
		StreamOperator <?> inOp = new MemSourceStreamOp(data_df, "f0 string, f1 double");
		data_df = Arrays.asList(
			Row.of("1", "value1"),
			Row.of("2", "value2"),
			Row.of("5", "value5")
		);
		BatchOperator <?> modelOp = new MemSourceBatchOp(data_df, "key_col string, value_col string");
		new Lookup()
			.setModelData(modelOp)
			.setMapKeyCols("key_col")
			.setMapValueCols("value_col")
			.setSelectedCols("f0")
			.transform(inOp)
			.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
|f0|f1|value_col|
|---|---|---|
|10|2.0|null|
|1|2.0|value1|
|-3|2.0|null|
|5|1.0|value5|
