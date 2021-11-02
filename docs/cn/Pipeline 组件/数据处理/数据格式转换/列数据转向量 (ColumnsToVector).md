# 列数据转向量 (ColumnsToVector)
Java 类名：com.alibaba.alink.pipeline.dataproc.format.ColumnsToVector

Python 类名：ColumnsToVector


## 功能介绍
将数据格式从 Columns 转成 Vector


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | null |
| vectorSize | 向量长度 | 向量长度 | Long |  | -1 |

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
 
op = ColumnsToVector()\
    .setSelectedCols(["f0", "f1"])\
    .setReservedCols(["row"])\
    .setVectorCol("vec")\
    .setVectorSize(5)\
    .transform(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.format.ColumnsToVector;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ColumnsToVectorTest {
	@Test
	public void testColumnsToVector() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		BatchOperator <?> op = new ColumnsToVector()
			.setSelectedCols("f0", "f1")
			.setReservedCols("row")
			.setVectorCol("vec")
			.setVectorSize(5)
			.transform(data);
		op.print();
	}
}
```

### 运行结果
    
|row|vec|
|---|-----|
|1|$5$1.0 2.0|
|2|$5$4.0 8.0|
    
