# 向量转张量 (VectorToTensorStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.VectorToTensorStreamOp

Python 类名：VectorToTensorStreamOp


## 功能介绍
转换向量类型为张量类型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |
| handleInvalidMethod | 处理无效值的方法 | 处理无效值的方法，可取 error, skip | String |  | "ERROR", "SKIP" | "ERROR" |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| tensorDataType | 要转换的张量数据类型 | 要转换的张量数据类型。 | String |  | "FLOAT", "DOUBLE", "INT", "LONG", "BOOLEAN", "BYTE", "UBYTE", "STRING" |  |
| tensorShape | 张量形状 | 张量的形状，数组类型。 | Long[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ['0.0 0.1 1.0 1.1 2.0 2.1']
])

stream_data = StreamOperator.fromDataframe(df_data, schemaStr = 'vec string')

stream_data.link(VectorToTensorStreamOp().setSelectedCol("vec")).print()

StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class VectorToTensorStreamOpTest {

	@Test
	public void testVectorToTensorStreamOp() throws Exception {
		List <Row> data = Collections.singletonList(Row.of("0.0 0.1 1.0 1.1 2.0 2.1"));

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(data, "vec string");

		memSourceStreamOp
			.link(
				new VectorToTensorStreamOp()
					.setSelectedCol("vec")
			)
			.print();

		StreamOperator.execute();
	}
}
```

### 运行结果

| vec                              |
|----------------------------------|
| DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1 |
