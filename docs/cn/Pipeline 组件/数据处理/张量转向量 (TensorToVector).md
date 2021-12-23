# 张量转向量 (TensorToVector)
Java 类名：com.alibaba.alink.pipeline.dataproc.TensorToVector

Python 类名：TensorToVector


## 功能介绍
转换张量类型为向量类型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| convertMethod | 转换方法 | 张量转换为向量的方法，可取 flatten, sum, mean, max, min. | String |  | "FLATTEN" |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ['DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1']
])

batch_data = BatchOperator.fromDataframe(df_data, schemaStr = 'tensor string')

TensorToVector().setSelectedCol("tensor").transform(batch_data).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TensorToVectorTest {

	@Test
	public void testTensorToVector() throws Exception {
		List <Row> data = Collections.singletonList(Row.of("DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1"));

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "tensor string");

		new TensorToVector()
			.setSelectedCol("tensor")
			.transform(memSourceBatchOp)
			.print();
	}
}

```

### 运行结果

| tensor                  |
|-------------------------|
| 0.0 0.1 1.0 1.1 2.0 2.1 |
