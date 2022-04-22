# 向量长度检验 (VectorSizeHintStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.vector.VectorSizeHintStreamOp

Python 类名：VectorSizeHintStreamOp


## 功能介绍
取出Vector 的size进行检测，并进行处理。

指定size大小和处理策略（抛异常error和跳过skip），当大小不匹配时，触发处理策略。
## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |
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
data = StreamOperator.fromDataframe(df, schemaStr="vec string")
VectorSizeHintStreamOp().setSelectedCol("vec").setOutputCol("vec_hint").setHandleInvalidMethod("Skip").setSize(3).linkFrom(data).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.vector.VectorSizeHintStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorSizeHintStreamOpTest {
	@Test
	public void testVectorSizeHintStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("$8$1:3,2:4,4:7"),
			Row.of("$8$2:4,4:5")
		);
		StreamOperator <?> data = new MemSourceStreamOp(df, "vec string");
		new VectorSizeHintStreamOp().setSelectedCol("vec").setOutputCol("vec_hint").setHandleInvalidMethod("Skip")
			.setSize(3).linkFrom(data).print();
		StreamOperator.execute();
	}
}
```
### 运行结果
|vec|vec_hint|
|---|--------|
|$8$1:3,2:4,4:7|$8$1:3.0 2:4.0 4:7.0|
|$8$2:4,4:5|$8$2:4.0 4:5.0|
