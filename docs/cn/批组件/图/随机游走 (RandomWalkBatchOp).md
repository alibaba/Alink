# 随机游走 (RandomWalkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.RandomWalkBatchOp

Python 类名：RandomWalkBatchOp


## 功能介绍
RandomWalk是deepwalk中描述随机游走的一种算法。
在给定的图上，每次迭代过程中，点都会转移到它的邻居上，转移到每个邻居的概率和连接这两个点的边的权重相关。
通过这样的随机游走可以获得固定长度的随机游走序列，这可以类比自然语言中的句子。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| sourceCol | 起始点列名 | 用来指定起始点列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| targetCol | 中止点点列名 | 用来指定中止点列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| walkLength | 游走的长度 | 随机游走完向量的长度 | Integer | ✓ |  |  |
| walkNum | 路径数目 | 每一个起始点游走出多少条路径 | Integer | ✓ |  |  |
| delimiter | 分隔符 | 用来分割字符串 | String |  |  | " " |
| isToUndigraph | 是否转无向图 | 选为true时，会将当前图转成无向图，然后再游走 | Boolean |  |  | false |
| isWeightedSampling | 是否为加权采样 | 该算法支持加权采样和随机采样两种采样方式 | Boolean |  |  | true |
| samplingMethod | 起始点列名 | 用来指定起始点列 | String |  |  | "ALIAS" |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1, 1, 1.0],
    [1, 2, 1.0],
    [2, 3, 1.0],
    [3, 4, 1.0],
    [4, 2, 1.0],
    [3, 1, 1.0],
    [2, 4, 1.0],
    [4, 1, 1.0]])

graph = BatchOperator.fromDataframe(df, schemaStr="start int, dest int, weight double")

RandomWalkBatchOp() \
			.setWalkNum(5) \
			.setWalkLength(20) \
			.setSourceCol("start") \
			.setTargetCol("dest") \
			.setIsToUndigraph(True) \
			.setWeightCol("weight").linkFrom(graph).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.RandomWalkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RandomWalkBatchOpTest {
	@Test
	public void testRandomWalkBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, 1, 1.0),
			Row.of(1, 2, 1.0),
			Row.of(2, 3, 1.0),
			Row.of(3, 4, 1.0),
			Row.of(4, 2, 1.0),
			Row.of(3, 1, 1.0),
			Row.of(2, 4, 1.0)
		);
		BatchOperator <?> graph = new MemSourceBatchOp(df, "start int, dest int, weight double");
		new RandomWalkBatchOp()
			.setWalkNum(5)
			.setWalkLength(20)
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(true)
			.setWeightCol("weight").linkFrom(graph).print();
	}
}
```
### 运行结果
|path|
|----|
|2 4 1 1 2 1 3 2 1 3 1 1 2 4 2 4 3 1 4 3|
|1 1 4 2 3 4 3 1 2 4 2 3 1 4 1 1 1 2 1 2|
|1 3 4 1 2 4 2 3 4 2 1 2 1 3 2 1 2 1 3 4|
|3 2 3 1 1 3 1 3 1 4 2 3 2 1 1 1 4 2 3 2|
|4 3 1 4 1 4 3 1 2 3 2 3 4 3 4 1 4 3 2 1|
|2 3 4 3 2 4 3 2 1 4 2 3 1 4 3 1 2 4 1 4|
|3 2 4 1 3 2 1 2 4 3 1 3 1 2 3 1 3 2 3 4|
|4 2 4 2 3 4 1 1 1 4 2 4 3 4 3 2 1 2 3 2|
|1 3 1 2 3 2 1 4 1 3 2 1 3 2 3 1 4 2 1 1|
|2 1 2 4 2 1 2 4 3 2 3 2 4 3 1 3 1 2 3 4|
|3 1 1 2 4 1 4 2 4 1 3 2 4 1 3 2 1 2 1 3|
|3 1 1 4 3 1 3 4 2 4 3 1 3 1 4 2 1 3 1 1|
|4 2 1 3 4 2 3 1 3 4 2 3 4 3 2 4 2 3 1 4|
|4 1 1 4 3 1 4 3 1 3 4 3 4 2 1 3 2 3 1 3|
|1 4 2 1 3 4 1 2 3 2 4 1 4 1 2 3 4 3 2 1|
|3 4 1 4 1 3 2 1 4 2 3 4 1 1 3 2 3 2 4 1|
|2 1 4 1 1 3 1 2 1 1 3 2 1 3 1 3 4 2 3 2|
|2 3 2 1 1 4 3 4 1 1 3 2 1 2 1 2 1 2 3 1|
|4 3 4 3 4 2 3 4 3 4 1 3 1 2 1 3 2 4 1 2|
|1 2 4 3 2 4 1 1 2 1 2 4 3 1 2 3 4 3 1 4|
