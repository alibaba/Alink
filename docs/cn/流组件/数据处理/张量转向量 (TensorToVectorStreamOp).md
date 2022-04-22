# 张量转向量 (TensorToVectorStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.TensorToVectorStreamOp

Python 类名：TensorToVectorStreamOp


## 功能介绍
转换张量类型为向量类型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR] |  |
| convertMethod | 转换方法 | 张量转换为向量的方法，可取 flatten, sum, mean, max, min. | String |  | "FLATTEN", "SUM", "MEAN", "MAX", "MIN" | "FLATTEN" |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ['DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1']
])

stream_data = StreamOperator.fromDataframe(df_data, schemaStr = 'tensor string')

stream_data.link(TensorToVectorStreamOp().setSelectedCol("tensor")).print()

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

public class TensorToVectorStreamOpTest {

	@Test
	public void testTensorToVectorStreamOp() throws Exception {
		List <Row> data = Collections.singletonList(Row.of("DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1"));

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(data, "tensor string");

		memSourceStreamOp
			.link(
				new TensorToVectorStreamOp()
					.setSelectedCol("tensor")
			)
			.print();

		StreamOperator.execute();
	}
}
```

### 运行结果

| tensor                  |
|-------------------------|
| 0.0 0.1 1.0 1.1 2.0 2.1 |
