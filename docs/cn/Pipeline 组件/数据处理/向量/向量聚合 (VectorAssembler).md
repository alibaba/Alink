# 向量聚合 (VectorAssembler)
Java 类名：com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler

Python 类名：VectorAssembler


## 功能介绍
数据结构转换，将多列数据（可以是向量列也可以是数值列）转化为一列向量数据。

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
    ["0", "$6$1:2.0 2:3.0 5:4.3", "3.0 2.0 3.0"],
    ["1", "$8$1:2.0 2:3.0 7:4.3", "3.0 2.0 3.0"],
    ["2", "$8$1:2.0 2:3.0 7:4.3", "2.0 3.0"]
])
data = BatchOperator.fromDataframe(df, schemaStr="id string, c0 string, c1 string")

res = VectorAssembler()\
			.setSelectedCols(["c0", "c1"])\
			.setOutputCol("table2vec")
res.transform(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorAssemblerTest {
	@Test
	public void testVectorAssembler() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("0", "$6$1:2.0 2:3.0 5:4.3", "3.0 2.0 3.0"),
			Row.of("1", "$8$1:2.0 2:3.0 7:4.3", "3.0 2.0 3.0"),
			Row.of("2", "$8$1:2.0 2:3.0 7:4.3", "2.0 3.0")
		);
		MemSourceBatchOp data = new MemSourceBatchOp(df, "id string, c0 string, c1 string");
		VectorAssembler res = new VectorAssembler()
			.setSelectedCols("c0", "c1")
			.setOutputCol("table2vec");
		res.transform(data).print();
	}
}
```

### 运行结果

id|c0|c1|table2vec
---|---|---|---------
0|$6$1:2.0 2:3.0 5:4.3|3.0 2.0 3.0|$9$1:2.0 2:3.0 5:4.3 6:3.0 7:2.0 8:3.0
1|$8$1:2.0 2:3.0 7:4.3|3.0 2.0 3.0|$11$1:2.0 2:3.0 7:4.3 8:3.0 9:2.0 10:3.0
2|$8$1:2.0 2:3.0 7:4.3|2.0 3.0|$10$1:2.0 2:3.0 7:4.3 8:2.0 9:3.0
