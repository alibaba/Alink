# Node2Vec游走 (Node2VecWalkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.Node2VecWalkBatchOp

Python 类名：Node2VecWalkBatchOp


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| sourceCol | 起始点列名 | 用来指定起始点列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| targetCol | 中止点点列名 | 用来指定中止点列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| walkLength | 游走的长度 | 随机游走完向量的长度 | Integer | ✓ |  |  |
| walkNum | 路径数目 | 每一个起始点游走出多少条路径 | Integer | ✓ |  |  |
| delimiter | 分隔符 | 用来分割字符串 | String |  |  | " " |
| isToUndigraph | 是否转无向图 | 选为true时，会将当前图转成无向图，然后再游走 | Boolean |  |  | false |
| p | 算法参数P | 控制随机游走序列的跳转概率 | Double |  |  | 1.0 |
| q | 算法参数Q | 控制随机游走序列的跳转概率 | Double |  |  | 1.0 |
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

source = BatchOperator.fromDataframe(df, schemaStr="start int, dest int, weight double")

n2vWalkBatchOp = Node2VecWalkBatchOp() \
                .setWalkNum(4) \
                .setWalkLength(50) \
                .setDelimiter(",") \
                .setSourceCol("start") \
                .setTargetCol("dest") \
                .setIsToUndigraph(True) \
                .setWeightCol("weight")

n2vWalkBatchOp.linkFrom(source).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.Node2VecWalkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class Node2VecWalkBatchOpTest {
	@Test
	public void testNode2VecWalkBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1, 1, 1.0),
			Row.of(1, 2, 1.0),
			Row.of(2, 3, 1.0),
			Row.of(3, 4, 1.0),
			Row.of(4, 2, 1.0),
			Row.of(3, 1, 1.0),
			Row.of(2, 4, 1.0)
		);
		BatchOperator <?> source = new MemSourceBatchOp(df, "start int, dest int, weight double");
		BatchOperator <?> n2vWalkBatchOp = new Node2VecWalkBatchOp()
			.setWalkNum(4)
			.setWalkLength(50)
			.setDelimiter(",")
			.setSourceCol("start")
			.setTargetCol("dest")
			.setIsToUndigraph(true)
			.setWeightCol("weight");
		n2vWalkBatchOp.linkFrom(source).print();
	}
}
```
### 运行结果

| path |
| ---- |
|3,2,1,1,4,2,3,4,2,1,3,1,1,1,3,1,3,4,1,2,4,3,2,1,1,3,2,4,3,4,1,4,2,1,2,1,4,3,1,2,1,3,4,2,4,3,2,3,4,1|
|2,3,2,4,2,1,2,3,2,3,2,4,3,1,2,3,4,2,4,2,3,2,3,2,4,2,1,3,1,4,1,1,4,2,1,2,4,1,3,1,1,3,4,2,4,2,3,4,2,4|
|4,2,1,4,1,1,1,2,3,4,2,3,2,3,2,4,1,2,3,2,1,2,4,3,1,1,2,4,3,4,2,4,1,2,4,3,1,4,2,4,2,1,3,4,2,1,2,4,3,4|
|4,3,1,1,1,3,4,3,4,1,2,4,2,3,2,1,1,1,2,3,4,1,2,4,3,4,3,1,4,2,3,2,4,1,1,1,3,1,3,2,4,2,4,3,1,1,1,3,2,1|
|4,3,1,1,3,2,3,1,4,1,2,1,3,4,3,4,2,1,2,4,2,3,4,2,4,2,4,2,3,1,4,3,2,4,1,2,3,2,1,1,3,1,1,4,1,4,1,4,1,2|
|1,3,4,1,2,3,1,3,4,2,1,4,3,2,1,3,1,4,1,4,3,1,4,2,1,2,3,4,3,4,2,3,4,3,4,1,1,1,1,2,4,1,2,4,1,2,4,2,3,1|
|3,4,1,4,1,2,1,3,2,4,2,1,2,3,1,4,1,2,4,2,4,3,4,3,2,3,2,1,2,1,1,1,4,2,3,4,1,1,4,2,3,4,3,1,4,3,4,1,4,3|
|2,3,1,3,4,1,1,1,4,3,4,1,2,3,2,1,1,3,4,3,2,3,1,3,4,3,2,1,4,3,1,1,2,4,2,1,3,1,3,1,2,3,1,4,3,2,1,2,1,1|
|1,2,1,3,1,4,2,3,2,1,3,4,2,4,3,4,2,4,2,1,4,3,2,3,4,1,4,2,1,4,1,3,4,2,1,4,1,4,3,1,3,2,4,1,1,4,1,1,2,3|
|1,2,4,2,1,2,4,3,4,3,4,3,1,2,1,2,3,2,3,4,2,4,3,2,3,2,3,1,1,1,4,2,1,4,1,2,1,2,1,1,2,4,2,1,4,2,3,1,1,4|
|3,4,1,1,1,2,4,2,4,1,1,1,2,3,2,4,2,1,2,3,4,1,1,3,4,1,2,4,3,2,1,1,4,1,2,1,2,4,2,4,1,3,4,2,1,3,4,3,2,1|
|3,2,3,4,3,1,2,4,2,4,1,2,3,4,3,1,2,4,3,2,3,2,3,2,3,4,3,2,4,3,4,2,4,3,1,1,4,1,4,1,3,1,2,3,4,1,4,3,4,2|
|1,4,2,4,1,4,1,2,1,1,2,3,1,4,3,4,1,3,1,3,4,2,4,3,2,3,2,3,4,1,3,2,3,4,2,3,4,1,2,4,2,3,4,3,2,3,1,4,2,3|
|2,4,3,1,1,3,1,2,4,2,4,3,2,1,2,4,3,2,3,1,1,3,2,1,3,1,4,1,3,1,1,1,1,2,1,3,1,1,3,4,2,1,2,1,1,2,4,3,1,2|
|4,2,1,4,3,2,1,1,3,1,4,1,3,2,4,2,4,2,4,3,4,1,3,4,2,3,2,1,3,1,1,1,4,2,1,2,4,1,4,2,1,4,1,2,4,2,1,4,1,2|
|2,4,1,4,3,2,1,3,1,3,1,4,1,2,4,1,3,4,2,1,1,3,1,3,1,1,1,1,1,2,4,1,4,3,1,2,1,2,4,1,3,4,1,2,3,1,4,2,4,3|
