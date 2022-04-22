# 向量聚合 (VectorAssemblerBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp

Python 类名：VectorAssemblerBatchOp


## 功能介绍
* 数据结构转换组件，将Table格式的数据转成tensor格式数据。
* 支持table中的多个 vector 列和数值列合并成一个vector 列。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| handleInvalidMethod | 处理无效值的方法 | 处理无效值的方法，可取 error, skip | String |  | "ERROR", "SKIP" | "ERROR" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]
])

data = BatchOperator.fromDataframe(df, schemaStr="f0 int, f1 int, f2 int")
colnames = ["f0","f1","f2"]
VectorAssemblerBatchOp().setSelectedCols(colnames)\
.setOutputCol("out").linkFrom(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorAssemblerBatchOpTest {
	@Test
	public void testVectorAssemblerBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1),
			Row.of(5, 3, 3)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 int, f1 int, f2 int");
		new VectorAssemblerBatchOp().setSelectedCols("f0", "f1", "f2")
			.setOutputCol("out").linkFrom(data).print();
	}
}
```

### 运行结果
f0|f1|f2|out
---|---|---|---
2|1|1|2.0 1.0 1.0
3|2|1|3.0 2.0 1.0
4|3|2|4.0 3.0 2.0
2|4|1|2.0 4.0 1.0
2|2|1|2.0 2.0 1.0
4|3|2|4.0 3.0 2.0
1|2|1|1.0 2.0 1.0
5|3|3|5.0 3.0 3.0
