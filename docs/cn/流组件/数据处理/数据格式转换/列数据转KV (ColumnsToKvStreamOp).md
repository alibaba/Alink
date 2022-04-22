# 列数据转KV (ColumnsToKvStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.format.ColumnsToKvStreamOp

Python 类名：ColumnsToKvStreamOp


## 功能介绍
将数据格式从 Columns 转成 Kv


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| kvCol | KV列名 | KV列的列名 | String | ✓ |  |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR", "SKIP" | "ERROR" |
| kvColDelimiter | 分隔符 | 当输入数据为稀疏格式时，key-value对之间的分隔符 | String |  |  | "," |
| kvValDelimiter | 分隔符 | 当输入数据为稀疏格式时，key和value的分割符 | String |  |  | ":" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
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

data = StreamOperator.fromDataframe(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double")

op = ColumnsToKvStreamOp()\
    .setSelectedCols(["f0", "f1"])\
    .setReservedCols(["row"]).setKvCol("kv")\
    .linkFrom(data)
op.print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.format.ColumnsToKvStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ColumnsToKvStreamOpTest {
	@Test
	public void testColumnsToKvStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		StreamOperator <?> data = new MemSourceStreamOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		StreamOperator <?> op = new ColumnsToKvStreamOp()
			.setSelectedCols("f0", "f1")
			.setReservedCols("row").setKvCol("kv")
			.linkFrom(data);
		op.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
    
|row|kv|
|---|---|
|1|f0:1.0,f1:2.0|
|2|f0:4.0,f1:8.0|
    
