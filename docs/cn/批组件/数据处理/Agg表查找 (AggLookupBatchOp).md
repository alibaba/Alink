# Agg表查找 (AggLookupBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.AggLookupBatchOp

Python 类名：AggLookupBatchOp


## 功能介绍
支持对数据中的query进行聚合查找功能，支持CONCAT、AVG、SUM、MAX、MIN等聚合方式，具体使用方式参考示例。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |
| delimiter | 分隔符 | 用来分割字符串 | String |  | " " |
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
    ["1,2,3,4", "1,2,3,4", "1,2,3,4", "1,2,3,4", "1,2,3,4"]
])


inOp = BatchOperator.fromDataframe(df, schemaStr='c0 string, c1 string, c2 string, c3 string, c4 string')

df2 = pd.DataFrame([
    ["1", "1.0,2.0,3.0,4.0"], ["2", "2.0,3.0,4.0,5.0"], ["3", "3.0,2.0,3.0,4.0"],["4", "4.0,5.0,6.0,5.0"]
])

modelOp = BatchOperator.fromDataframe(df2, schemaStr="id string, vec string")

AggLookupBatchOp() \
	.setClause("CONCAT(c0,3) as e0, AVG(c1) as e1, SUM(c2) as e2,MAX(c3) as e3,MIN(c4) as e4") \
	.setDelimiter(",") \
	.setReservedCols([]) \
    .linkFrom(modelOp, inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.AggLookupBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggLookupBatchOpTest {
	@Test
	public void testAggLookupBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1,2,3,4", "1,2,3,4", "1,2,3,4", "1,2,3,4", "1,2,3,4")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "c0 string, c1 string, c2 string, c3 string, c4 string");
		List <Row> df2 = Arrays.asList(
			Row.of("1", "1.0,2.0,3.0,4.0"), Row.of("2", "2.0,3.0,4.0,5.0"), Row.of("3", "3.0,2.0,3.0,4.0"),
			Row.of("4", "4.0,5.0,6.0,5.0")
		);
		BatchOperator <?> modelOp = new MemSourceBatchOp(df2, "id string, vec string");
		new AggLookupBatchOp()
			.setClause("CONCAT(c0,3) as e0, AVG(c1) as e1, SUM(c2) as e2,MAX(c3) as e3,MIN(c4) as e4")
			.setDelimiter(",")
			.linkFrom(modelOp, inOp).print();
	}
}
```

### 运行结果
| e0                                              | e1              | e2                  | e3              | e4              |
| ----------------------------------------------- | --------------- | ------------------- | --------------- | --------------- |
| 1.0 2.0 3.0 4.0 2.0 3.0 4.0 5.0 3.0 2.0 3.0 4.0 | 2.5 3.0 4.0 4.5 | 10.0 12.0 16.0 18.0 | 4.0 5.0 6.0 5.0 | 1.0 2.0 3.0 4.0 |
