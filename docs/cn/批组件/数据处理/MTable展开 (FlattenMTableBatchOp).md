# MTable展开 (FlattenMTableBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.FlattenMTableBatchOp

Python 类名：FlattenMTableBatchOp


## 功能介绍
该组件将 MTable 展开成 Table。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [M_TABLE] |  |
| handleInvalidMethod | 处理无效值的方法 | 处理无效值的方法，可取 error, skip | String |  | "ERROR", "SKIP" | "ERROR" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |


## 代码示例
### Python 代码

```python
import numpy as np
import pandas as pd
from pyalink.alink import *

df_data = pd.DataFrame([
      ["a1", "11L", 2.2],
      ["a1", "12L", 2.0],
      ["a2", "11L", 2.0],
      ["a2", "12L", 2.0],
      ["a3", "12L", 2.0],
      ["a3", "13L", 2.0],
      ["a4", "13L", 2.0],
      ["a4", "14L", 2.0],
      ["a5", "14L", 2.0],
      ["a5", "15L", 2.0],
      ["a6", "15L", 2.0],
      ["a6", "16L", 2.0]
])

input = BatchOperator.fromDataframe(df_data, schemaStr='id string, f0 string, f1 double')

zip = GroupByBatchOp()\
	.setGroupByPredicate("id")\
	.setSelectClause("id, mtable_agg(f0, f1) as m_table_col")

flatten = FlattenMTableBatchOp()\
	.setReservedCols(["id"])\
	.setSelectedCol("m_table_col")\
	.setSchemaStr('f0 string, f1 int')

zip.linkFrom(input).link(flatten).print()
```
### Java 代码
```java
package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for gbdt.
 */

public class FlattenMTableTest extends AlinkTestBase {
	
	@Test
	public void test() throws Exception {
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of("a1", "11L", 2.2));

		rows.add(Row.of("a1", "12L", 2.0));
		rows.add(Row.of("a2", "11L", 2.0));
		rows.add(Row.of("a2", "12L", 2.0));
		rows.add(Row.of("a3", "12L", 2.0));
		rows.add(Row.of("a3", "13L", 2.0));
		rows.add(Row.of("a4", "13L", 2.0));
		rows.add(Row.of("a4", "14L", 2.0));
		rows.add(Row.of("a5", "14L", 2.0));
		rows.add(Row.of("a5", "15L", 2.0));
		rows.add(Row.of("a6", "15L", 2.0));
		rows.add(Row.of("a6", "16L", 2.0));

		BatchOperator input = new MemSourceBatchOp(rows, "id string, f0 string, f1 double");

		GroupByBatchOp zip = new GroupByBatchOp()
			.setGroupByPredicate("id")
			.setSelectClause("id, mtable_agg(f0, f1) as m_table_col");

		FlattenMTableBatchOp flatten = new FlattenMTableBatchOp()
			.setReservedCols("id")
			.setSelectedCol("m_table_col")
			.setSchemaStr("f0 string, f1 int");

		zip.linkFrom(input).link(flatten).print();
	}
}
```

### 运行结果

id|f0|f1
---|---|---
a2|11L|2
a2|12L|2
a4|13L|2
a4|14L|2
a5|14L|2
a5|15L|2
a1|11L|2
a1|12L|2
a3|12L|2
a3|13L|2
a6|15L|2
a6|16L|2
