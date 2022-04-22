# 向量长度检验 (VectorSizeHint)
Java 类名：com.alibaba.alink.pipeline.dataproc.vector.VectorSizeHint

Python 类名：VectorSizeHint


## 功能介绍
取出Vector 的size进行检测，并进行处理。
## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| size | 向量大小 | 用于判断向量的大小是否和设置的一致 | Integer | ✓ |  |  |
| handleInvalidMethod | 处理无效值的方法 | 处理无效值的方法，可取 error, skip | String |  | "ERROR", "SKIP" | "ERROR" |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["$8$1:3,2:4,4:7"],
    ["$8$2:4,4:5"]
])
data = BatchOperator.fromDataframe(df, schemaStr="vec string")
model = VectorSizeHint().setSelectedCol("vec").setOutputCol("vec_hint").setHandleInvalidMethod("SKIP").setSize(8)
model.transform(data).collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorSizeHint;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorSizeHintTest {
	@Test
	public void testVectorSizeHint() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("$8$1:3,2:4,4:7"),
			Row.of("$8$2:4,4:5")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec string");
		VectorSizeHint model = new VectorSizeHint().setSelectedCol("vec").setOutputCol("vec_hint")
			.setHandleInvalidMethod("SKIP").setSize(8);
		model.transform(data).print();
	}
}
```
### 运行结果
|vec|vec_hint|
|---|--------|
|$8$1:3,2:4,4:7|$8$1:3.0 2:4.0 4:7.0|
|$8$2:4,4:5|$8$2:4.0 4:5.0|
