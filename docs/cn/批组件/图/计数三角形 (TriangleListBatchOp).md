# 计数三角形 (TriangleListBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.TriangleListBatchOp

Python 类名：TriangleListBatchOp


## 功能介绍

对于给定的图，计算图中边所能构成的三角形，并将其输出。

对网络图中进行三角形个数计数可以根据三角形数量反应网络中的稠密程度和质量。
可以用于社区发现，如微博中你关注的人也关注你，大家的关注关系中有很多三角形，说明社区很强很稳定，大家联系比较紧密；如果一个人只关注了很多人，却没有形成三角形，则说明社交群体很小很松散。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([[1.0,2.0],\
[1.0,3.0],\
[1.0,4.0],\
[1.0,5.0],\
[1.0,6.0],\
[2.0,3.0],\
[4.0,3.0],\
[5.0,4.0],\
[5.0,6.0],\
[5.0,7.0],\
[6.0,7.0]])

data = BatchOperator.fromDataframe(df, schemaStr="source double, target double")
TriangleListBatchOp()\
    .setEdgeSourceCol("source")\
    .setEdgeTargetCol("target")\
    .linkFrom(data)\
    .print()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class TriangleListBatchOpTest extends AlinkTestBase {
	@Test
	public void testTriangleList() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1.0, 2.0),
			Row.of(1.0, 3.0),
			Row.of(1.0, 4.0),
			Row.of(1.0, 5.0),
			Row.of(1.0, 6.0),
			Row.of(2.0, 3.0),
			Row.of(4.0, 3.0),
			Row.of(5.0, 4.0),
			Row.of(5.0, 6.0),
			Row.of(5.0, 7.0),
			Row.of(6.0, 7.0)
		};
		BatchOperator inData = new MemSourceBatchOp(Arrays.asList(rows), "source double,target double");

		TriangleListBatchOp op = new TriangleListBatchOp()
			.setEdgeSourceCol("source")
			.setEdgeTargetCol("target").linkFrom(inData);
		op.print();
	}
}
```

### 运行结果


|node1|node2|node3|
|-----|-----|-----|
|6.0000|5.0000|1.0000|
|4.0000|5.0000|1.0000|
|7.0000|5.0000|6.0000|
|4.0000|1.0000|3.0000|
|2.0000|1.0000|3.0000|
