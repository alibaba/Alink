# 向量函数 (VectorFunction)
Java 类名：com.alibaba.alink.pipeline.dataproc.vector.VectorFunction

Python 类名：VectorFunction


## 功能介绍
* 获取一个向量的最大值、最小值，或者最大值、最小值的索引，或者对向量做尺度变换, 求NormL2, 求NormL1, 求NormL2Square, Normalize。
* 支持稀疏和稠密两种 Vector。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| funcName | 函数名字 | 函数操作名称, 可取max（最大值）, min（最小值）, argMax（最大值索引）, argMin（最小值索引）, scale（尺度变换）, NormL2, NormL1, NormL2Square, Normalize | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| WithVariable | Not available! | Not available! | String |  |  |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1,"16.3, 1.1, 1.1"],
    [2,"16.8, 1.4, 1.5"],
    [3,"19.2, 1.7, 1.8"],
    [4,"10.0, 1.7, 1.7"],
    [5,"19.5, 1.8, 1.9"],
    [6,"20.9, 1.8, 1.8"],
    [7,"21.1, 1.9, 1.8"],
    [8,"20.9, 2.0, 2.1"],
    [9,"20.3, 2.3, 2.4"],
    [10,"22.0, 2.4, 2.5"]
])
opData = BatchOperator.fromDataframe(df, schemaStr="id bigint, vec string")
result = VectorFunction().setSelectedCol("vec")\
.setOutputCol("out").setFuncName("max").transform(opData)
result.collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;
import com.alibaba.alink.pipeline.dataproc.vector.VectorFunction;
import com.alibaba.alink.operator.stream.BatchOperator;
import com.alibaba.alink.operator.stream.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class VectorFunctionTest extends AlinkTestBase {

	@Test
	public void testVectorFunction() throws Exception {
		List <Row> df = new ArrayList <>();
		df.add(Row.of(1, "16.3, 1.1, 1.1"));
		df.add(Row.of(2, "16.8, 1.4, 1.5"));
		df.add(Row.of(3, "19.2, 1.7, 1.8"));
		df.add(Row.of(4, "10.0, 1.7, 1.7"));
		df.add(Row.of(5, "19.5, 1.8, 1.9"));
		df.add(Row.of(6, "20.9, 1.8, 1.8"));
		df.add(Row.of(7, "21.1, 1.9, 1.8"));
		df.add(Row.of(8, "20.9, 2.0, 2.1"));
		df.add(Row.of(9, "20.3, 2.3, 2.4"));
		df.add(Row.of(10, "22.0, 2.4, 2.5"));

		BatchOperator<?> streamData =  new MemSourceBatchOp(df, "id int, vec string");

		new VectorFunction().setSelectedCol("vec")
			.setOutputCol("out").setFuncName("max").transform(streamData).print();
	}
}
```
### 运行结果
id | vec | out
---|-----|---
1|16.3, 1.1, 1.1|16.3
2|16.8, 1.4, 1.5|16.8
3|19.2, 1.7, 1.8|19.2
4|10.0, 1.7, 1.7|10.0
5|19.5, 1.8, 1.9|19.5
6|20.9, 1.8, 1.8|20.9
7|21.1, 1.9, 1.8|21.1
8|20.9, 2.0, 2.1|20.9
9|20.3, 2.3, 2.4|20.3
10|22.0, 2.4, 2.5|22.0

