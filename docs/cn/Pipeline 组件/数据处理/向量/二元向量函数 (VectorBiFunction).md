# 二元向量函数 (VectorBiFunction)
Java 类名：com.alibaba.alink.pipeline.dataproc.vector.VectorBiFunction

Python 类名：VectorBiFunction


## 功能介绍
* 对两个向量进行操作的函数，支持minus(减),plus(加),dot(内积),merge(拼接),EuclidDistance(欧式距离),Cosine(cos值), ElementWiseMultiply(点乘)。
* 支持稀疏和稠密两种 Vector。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| biFuncName | 函数名字 | 函数操作名称, 可取minus(减),plus(加),dot(内积),merge(拼接),EuclidDistance(欧式距离),Cosine(cos值), ElementWiseMultiply(点乘). | String | ✓ |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["1 2 3", "2 3 4"]
])
data = BatchOperator.fromDataframe(df, schemaStr="vec1 string, vec2 string")
VectorBiFunction() \
		.setSelectedCols(["vec1", "vec2"]) \
		.setBiFuncName("minus").setOutputCol("vec_minus").transform(data).print();
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorBiFunction;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ToVectorBiFunctionTest extends AlinkTestBase {
	@Test
	public void testBiVectorFunction() throws Exception {
		List <Row> df = new ArrayList <>();
		df.add(Row.of("1 2 3", "2 3 4"));
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec1 string, vec2 string");
		 new VectorBiFunction()
			.setSelectedCols("vec1", "vec2")
			.setBiFuncName("minus")
			.setOutputCol("vec_minus").transform(data).print();
	}
}
```
### 运行结果
vec1 | vec2 | vec_minus
---|-----|---
1 2 3|2 3 4|-1.0 -1.0 -1.0

