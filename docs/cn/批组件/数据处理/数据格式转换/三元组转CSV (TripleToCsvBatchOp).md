# 三元组转CSV (TripleToCsvBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.format.TripleToCsvBatchOp

Python 类名：TripleToCsvBatchOp


## 功能介绍
将数据格式从 Triple 转成 Csv

三元组转换为key value对，setTripleRowCol 设置数据行信息的列名，这一列值相同的数据，会被合并成一个数据
setTripleColumnCol 设置为Column名称所在列名
setTripleValueCol 设置为Column值所在列名
setSchemaStr 设置输出的csv的Schema格式

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| csvCol | CSV列名 | CSV列的列名 | String | ✓ |  |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| tripleColumnCol | 三元组结构中列信息的列名 | 三元组结构中列信息的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| tripleValueCol | 三元组结构中数据信息的列名 | 三元组结构中数据信息的列名 | String | ✓ |  |  |
| csvFieldDelimiter | 字段分隔符 | 字段分隔符 | String |  |  | "," |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR", "SKIP" | "ERROR" |
| quoteChar | 引号字符 | 引号字符 | Character |  |  | "\"" |
| tripleRowCol | 三元组结构中行信息的列名 | 三元组结构中行信息的列名 | String |  |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1,'f1',1.0],
    [1,'f2',2.0],
    [2,'f1',4.0],
    [2,'f2',8.0]])

data = BatchOperator.fromDataframe(df, schemaStr="row double, col string, val double")

op = TripleToCsvBatchOp()\
    .setTripleRowCol("row")\
    .setTripleColumnCol("col")\
    .setTripleValueCol("val")\
    .setCsvCol("csv")\
    .setSchemaStr("f1 string, f2 string")\
    .linkFrom(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.TripleToCsvBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TripleToCsvBatchOpTest {
	@Test
	public void testTripleToCsvBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, "f1", 1.0),
			Row.of(1, "f2", 2.0),
			Row.of(2, "f1", 4.0),
			Row.of(2, "f2", 8.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "row int, col string, val double");
		BatchOperator <?> op = new TripleToCsvBatchOp()
			.setTripleRowCol("row")
			.setTripleColumnCol("col")
			.setTripleValueCol("val")
			.setCsvCol("csv")
			.setSchemaStr("f1 string, f2 string")
			.linkFrom(data);
		op.print();
	}
}
```

### 运行结果
    
|row|csv|
|---|-------|
|1|1.0,2.0|
|2|4.0,8.0|
    
