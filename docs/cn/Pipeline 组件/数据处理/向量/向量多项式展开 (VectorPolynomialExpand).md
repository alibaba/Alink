# 向量多项式展开 (VectorPolynomialExpand)
Java 类名：com.alibaba.alink.pipeline.dataproc.vector.VectorPolynomialExpand

Python 类名：VectorPolynomialExpand


## 功能介绍
对 Vector 进行多项式展开，组成一个新的Vector。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| degree | 多项式阶数 | 多项式的阶数，默认2 | Integer |  | [1, +inf) | 2 |
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
VectorPolynomialExpand().setSelectedCol("vec").setOutputCol("vec_out").transform(data).collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorPolynomialExpand;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorPolynomialExpandTest {
	@Test
	public void testVectorPolynomialExpand() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("$8$1:3,2:4,4:7"),
			Row.of("$8$2:4,4:5")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec string");
		new VectorPolynomialExpand().setSelectedCol("vec").setOutputCol("vec_out").transform(data).print();
	}
}
```
### 运行结果
| vec            | vec_out                                 |
| -------------- | ---------------------------------------- |
| $8$1:3,2:4,4:7 | $44$2:3.0 4:9.0 5:4.0 7:12.0 8:16.0 14:7.0 16:21.0 17:28.0 19:49.0 |
| $8$2:4,4:5     | $44$5:4.0 8:16.0 14:5.0 17:20.0 19:25.0  |
