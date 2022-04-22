# 列数据转三元组 (ColumnsToTripleBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.format.ColumnsToTripleBatchOp

Python 类名：ColumnsToTripleBatchOp


## 功能介绍
将数据格式从 Columns 转成 Triple三元组数据

注意一条输入数据可能会产生多条输出数据。按照属性名称和属性值展开。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| tripleColumnValueSchemaStr | 三元组结构中列信息和数据信息的Schema | 三元组结构中列信息和数据信息的Schema | String | ✓ |  |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR", "SKIP" | "ERROR" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | [] |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ['1', '{"f0":"1.0","f1":"2.0"}', '$3$0:1.0 1:2.0', 'f0:1.0,f1:2.0', '1.0,2.0', 1.0, 2.0],
    ['2', '{"f0":"4.0","f1":"8.0"}', '$3$0:4.0 1:8.0', 'f0:4.0,f1:8.0', '4.0,8.0', 4.0, 8.0]])

data = BatchOperator.fromDataframe(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double")
    
op = ColumnsToTripleBatchOp()\
    .setSelectedCols(["f0", "f1"])\
    .setReservedCols(["row"])\
    .setTripleColumnValueSchemaStr("col string, val double")\
    .linkFrom(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.ColumnsToTripleBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ColumnsToTripleBatchOpTest {
	@Test
	public void testColumnsToTripleBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		BatchOperator <?> op = new ColumnsToTripleBatchOp()
			.setSelectedCols("f0", "f1")
			.setReservedCols("row")
			.setTripleColumnValueSchemaStr("col string, val double")
			.linkFrom(data);
		op.print();
	}
}
```

### 运行结果
    
|row|col|val|
    |---|---|---|
    |1|f0|1.0|
    |1|f1|2.0|
    |2|f0|4.0|
    |2|f1|8.0|
