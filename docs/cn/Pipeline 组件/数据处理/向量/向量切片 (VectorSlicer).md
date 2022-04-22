# 向量切片 (VectorSlicer)
Java 类名：com.alibaba.alink.pipeline.dataproc.vector.VectorSlicer

Python 类名：VectorSlicer


## 功能介绍
取出Vector 中的若干列，组成一个新的Vector。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| indices | 需要被提取的索引数组 | 需要被提取的索引数组 | int[] |  |  | null |
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
    ["1:3 2:4 4:7", 1],
    ["0:3 5:5", 3],
    ["2:4 4:5", 4]
])

data = BatchOperator.fromDataframe(df, schemaStr="vec string, id bigint")
vecSlice = VectorSlicer().setSelectedCol("vec").setOutputCol("vec_slice").setIndices([1,2,3])
vecSlice.transform(data).collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorSlicer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorSlicerTest {
	@Test
	public void testVectorSlicer() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1:3 2:4 4:7", 1),
			Row.of("0:3 5:5", 3),
			Row.of("2:4 4:5", 4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec string, id int");
		VectorSlicer vecSlice = new VectorSlicer().setSelectedCol("vec").setOutputCol("vec_slice").setIndices(
			new int[] {1, 2, 3});
		vecSlice.transform(data).print();
	}
}
```
### 运行结果

| vec         | id   | vec_slice      |
| ----------- | ---- | -------------- |
| 1:3,2:4,4:7 | 1    | $3$0:3.0 1:4.0 |
| 0:3,5:5     | 3    | $3$            |
| 2:4,4:5     | 4    | $3$1:4.0       |
