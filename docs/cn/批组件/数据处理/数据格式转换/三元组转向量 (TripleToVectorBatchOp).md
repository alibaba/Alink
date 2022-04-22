# 三元组转向量 (TripleToVectorBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.format.TripleToVectorBatchOp

Python 类名：TripleToVectorBatchOp


## 功能介绍
将数据格式从 Triple 转成 Vector

三元组转换为向量，setTripleRowCol 设置数据行信息的列名，这一列值相同的数据，会被合并成一个向量。
setTripleColumnCol 设置向量的索引所在列，数值为向量的索引，因此需要为整型，int或者long。
setTripleValueCol 设置向量的值所在列，因此该列必须为数值类型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| tripleColumnCol | 三元组结构中列信息的列名 | 三元组结构中列信息的列名 | String | ✓ | 所选列类型为 [INTEGER, LONG] |  |
| tripleValueCol | 三元组结构中数据信息的列名 | 三元组结构中数据信息的列名 | String | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR", "SKIP" | "ERROR" |
| tripleRowCol | 三元组结构中行信息的列名 | 三元组结构中行信息的列名 | String |  |  | null |
| vectorSize | 向量长度 | 向量长度 | Long |  |  | -1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1,1,1.0],
    [1,2,2.0],
    [2,1,4.0],
    [2,2,8.0]])

data = BatchOperator.fromDataframe(df, schemaStr="row double, col string, val double")

op = TripleToVectorBatchOp()\
    .setTripleRowCol("row")\
    .setTripleColumnCol("col")\
    .setTripleValueCol("val")\
    .setVectorCol("vec")\
    .setVectorSize(5)\
    .linkFrom(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.TripleToVectorBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TripleToVectorBatchOpTest {
	@Test
	public void testTripleToVectorBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, 1, 1.0),
			Row.of(1, 2, 2.0),
			Row.of(2, 1, 4.0),
			Row.of(2, 2, 8.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "row int, col int, val double");
		BatchOperator <?> op = new TripleToVectorBatchOp()
			.setTripleRowCol("row")
			.setTripleColumnCol("col")
			.setTripleValueCol("val")
			.setVectorCol("vec")
			.setVectorSize(5)
			.linkFrom(data);
		op.print();
	}
}
```

### 运行结果

row|vec
---|---
1|$5$1:1.0 2:2.0
2|$5$1:4.0 2:8.0
    
