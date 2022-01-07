# 转向量 (ToVectorStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.ToVectorStreamOp

Python 类名：ToVectorStreamOp


## 功能介绍
将输入列转换为向量类型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| handleInvalidMethod | 处理无效值的方法 | 处理无效值的方法，可取 error, skip | String |  | "ERROR" |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| vectorType | 要转换的Vector类型。 | 要转换的Vector类型。 | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ['1 0 3 4']
])

data = StreamOperator.fromDataframe(df_data, schemaStr = 'vec string')

ToVectorStreamOp().setSelectedCol("vec").setVectorType("SPARSE").linkFrom(data).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorType;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.ToVectorStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class ToVectorTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		final String vecStr = "1 0 3 4";
		Row[] rows = new Row[] {
			Row.of(vecStr)
		};
		MemSourceStreamOp data = new MemSourceStreamOp(
			rows, new String[] {"vec"}
		);
		new ToVectorStreamOp()
			.setSelectedCol("vec")
			.setVectorType(VectorType.SPARSE)
			.linkFrom(data)
			.print();

		StreamOperator.execute();
	}
}
```

### 运行结果

| vec                  |
|----------------------|
| $4$0:1.0 2:3.0 3:4.0 |
