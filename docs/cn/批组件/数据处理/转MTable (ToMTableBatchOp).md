# 转MTable (ToMTableBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.ToMTableBatchOp

Python 类名：ToMTableBatchOp


## 功能介绍
将输入列转换为MTable类型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
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

df_data = pd.DataFrame([
    ['{"data":{"col0":[1],"col1":["2"],"label":[0]},"schema":"col0 INT, col1 VARCHAR,label INT"}']
])

data = BatchOperator.fromDataframe(df_data, schemaStr = 'vec string')

ToMTableBatchOp().setSelectedCol("vec").linkFrom(data).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class ToVectorDemoTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		final String mTableStr = "{\"data\":{\"col0\":[1],\"col1\":[\"2\"],\"label\":[0]},\"schema\":\"col0 INT, col1 VARCHAR,label INT\"}";

		Row[] rows = new Row[] {
			Row.of(mTableStr)
		};

		MemSourceBatchOp data = new MemSourceBatchOp(
			rows, new String[] {"m_table"}
		);
		new ToMTableBatchOp().setSelectedCol("vec").linkFrom(data).print();
	}
}
```

### 运行结果

| vec                  |
|----------------------|
| {"data":{"col0":[1],"col1":["2"],"label":[0]},"schema":"col0 INT,col1 VARCHAR,label INT"} |
