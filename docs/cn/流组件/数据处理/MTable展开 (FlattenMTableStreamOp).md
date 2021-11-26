# MTable展开 (FlattenMTableStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.FlattenMTableStreamOp

Python 类名：FlattenMTableStreamOp


## 功能介绍
该组件将 MTable 流数据展开成 Table。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| handleInvalidMethod | 处理无效值的方法 | 处理无效值的方法，可取 error, skip | String |  | "ERROR" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


## 代码示例
### Python 代码

```python
import numpy as np
import pandas as pd
df_data = pd.DataFrame([
      ["a1", "{\"data\":{\"f0\":[\"11L\",\"12L\"],\"f1\":[2.0,2.0]},\"schema\":\"f0 VARCHAR,f1 DOUBLE\"}"],
      ["a1", "{\"data\":{\"f0\":[\"13L\",\"14L\"],\"f1\":[2.0,2.0]},\"schema\":\"f0 VARCHAR,f1 DOUBLE\"}"]
])

input = StreamOperator.fromDataframe(df_data, schemaStr='id string, mt string')

flatten = FlattenMTableStreamOp()\
	.setReservedCols(["id"])\
	.setSelectedCol("mt")\
	.setSchemaStr('f0 string, f1 int')

input.link(flatten).print()
StreamOperator.execute()
```
### Java 代码
```java
package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.FlattenMTableStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for gbdt.
 */

public class FlattenMTableTest extends AlinkTestBase {
	
	@Test
	public void test3() throws Exception {
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of("a1", "{\"data\":{\"f0\":[\"11L\",\"12L\"],\"f1\":[2.0,2.0]},\"schema\":\"f0 VARCHAR,f1 "
			+ "DOUBLE\"}"));
            rows.add(Row.of("a3", "{\"data\":{\"f0\":[\"13L\",\"14L\"],\"f1\":[2.0,2.0]},\"schema\":\"f0 VARCHAR,f1 "
                + "DOUBLE\"}"));
		StreamOperator <?> input = new MemSourceStreamOp(rows, "id string, mt string");

		FlattenMTableStreamOp flatten = new FlattenMTableStreamOp()
			.setReservedCols("id")
			.setSelectedCol("mt")
			.setSchemaStr("f0 string, f1 int");
		flatten.linkFrom(input).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

id|f0|f1
---|---|---
a3|13L|2
a1|11L|2
a1|12L|2
a3|14L|2
