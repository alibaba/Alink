# 三元组转JSON (TripleToJsonBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.format.TripleToJsonBatchOp

Python 类名：TripleToJsonBatchOp


## 功能介绍
将数据格式从 Triple 转成 Json

三元组转换为json，setTripleRowCol 设置数据行信息的列名，这一列值相同的数据，会被合并成一个数据
setTripleColumnCol 设置为json中key所在的列名
setTripleValueCol 设置为json中value所在的列名

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| tripleColumnCol | 三元组结构中列信息的列名 | 三元组结构中列信息的列名 | String | ✓ |  |
| tripleValueCol | 三元组结构中数据信息的列名 | 三元组结构中数据信息的列名 | String | ✓ |  |
| jsonCol | JSON列名 | JSON列的列名 | String | ✓ |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR" |
| tripleRowCol | 三元组结构中行信息的列名 | 三元组结构中行信息的列名 | String |  | null |

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

op = TripleToJsonBatchOp()\
    .setTripleRowCol("row")\
    .setTripleColumnCol("col")\
    .setTripleValueCol("val")\
    .setJsonCol("json")\
    .linkFrom(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.TripleToJsonBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TripleToJsonBatchOpTest {
	@Test
	public void testTripleToJsonBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, "f1", 1.0),
			Row.of(1, "f2", 2.0),
			Row.of(2, "f1", 4.0),
			Row.of(2, "f2", 8.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "row int, col string, val double");
		BatchOperator <?> op = new TripleToJsonBatchOp()
			.setTripleRowCol("row")
			.setTripleColumnCol("col")
			.setTripleValueCol("val")
			.setJsonCol("json")
			.linkFrom(data);
		op.print();
	}
}
```

### 运行结果
    
|row|json|
|---|----|
| 1 |{"f1":"1.0","f2":"2.0"}|
| 2 |{"f1":"4.0","f2":"8.0"}|
