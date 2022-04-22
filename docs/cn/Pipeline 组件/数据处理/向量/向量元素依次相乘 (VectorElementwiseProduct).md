# 向量元素依次相乘 (VectorElementwiseProduct)
Java 类名：com.alibaba.alink.pipeline.dataproc.vector.VectorElementwiseProduct

Python 类名：VectorElementwiseProduct


## 功能介绍
 Vector 中的每一个非零元素与scalingVector的每一个对应元素乘，返回乘积后的新vector。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| scalingVector | 尺度变化向量。 | 尺度的变化向量。 | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
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
vecEP = VectorElementwiseProduct().setSelectedCol("vec") \
	.setOutputCol("vec1") \
	.setScalingVector("$8$1:3.0 3:3.0 5:4.6")
vecEP.transform(data).collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorElementwiseProduct;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorElementwiseProductTest {
	@Test
	public void testVectorElementwiseProduct() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1:3 2:4 4:7", 1),
			Row.of("0:3 5:5", 3),
			Row.of("2:4 4:5", 4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec string, id int");
		VectorElementwiseProduct vecEP = new VectorElementwiseProduct().setSelectedCol("vec")
			.setOutputCol("vec1")
			.setScalingVector("$8$1:3.0 3:3.0 5:4.6");
		vecEP.transform(data).print();
	}
}
```
### 运行结果
| vec         | id   | vec1              |
| ----------- | ---- | ----------------- |
| 1:3,2:4,4:7 | 1    | 1:9.0 2:0.0 4:0.0 |
| 0:3,5:5     | 3    | 0:0.0 5:23.0      |
| 2:4,4:5     | 4    | 2:0.0 4:0.0       |
