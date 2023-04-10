# Line (LineBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.LineBatchOp

Python 类名：LineBatchOp


## 功能介绍

 line算法(Large-scale Information Network Embedding)是一种graph embedding算法，可以将网络中的每一个点表示成连续特征空间中的一个点向量。
 line算法有一阶相似度和二阶相似度两种描述方法，一阶相似度用于描述图中成对顶点之间的局部相似度，二阶相似度以两个顶点的邻域特征来描述顶点的相似度。
 line算法的论文：[LINE: Large-scale Information Network Embedding](https://arxiv.org/pdf/1503.03578.pdf)
## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| sourceCol | 起始点列名 | 用来指定起始点列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| targetCol | 中止点点列名 | 用来指定中止点列 | String | ✓ | 所选列类型为 [INTEGER, LONG, STRING] |  |
| batchSize | batch大小 | batch大小, 按行计算 | Integer |  | x >= 1 |  |
| isToUndigraph | 是否转无向图 | 选为true时，会将当前图转成无向图，然后再游走 | Boolean |  |  | false |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | x >= 1 | 100 |
| minRhoRate | 最小学习率的比例 | 最小学习率的比例 | Double |  | 0.0 <= x <= 1.0 | 0.001 |
| negative | 负采样大小 | 负采样大小 | Integer |  |  | 5 |
| order | 阶数 | 选择一阶优化或是二阶优化 | String |  | "FirstOrder", "SecondOrder" | "FirstOrder" |
| rho | 学习率 | 学习率 | Double |  | x >= 0.0 | 0.025 |
| sampleRatioPerPartition | 采样率 | 每轮迭代在每个partition上采样样本的比率 | Double |  | x >= 0.0 | 1.0 |
| vectorSize | embedding的向量长度 | embedding的向量长度 | Integer |  | x >= 1 | 100 |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([\
["1L", "5L", 1.],\
["2L", "5L", 1.],\
["3L", "5L", 1.],\
["4L", "5L", 1.],\
["1L", "6L", 1.],\
["2L", "6L", 1.],\
["3L", "6L", 1.],\
["4L", "6L", 1.],\
["7L", "6L", 15.],\
["7L", "8L", 1.],\
["7L", "9L", 1.],\
["7L", "10L", 1.]])

data = BatchOperator.fromDataframe(df, schemaStr="source string, target string, weight double")
line = LineBatchOp()\
    .setOrder("firstorder")\
    .setRho(.025)\
    .setVectorSize(5)\
    .setNegative(5)\
    .setIsToUndigraph(False)\
    .setMaxIter(20)\
    .setSampleRatioPerPartition(2.)\
    .setSourceCol("source")\
    .setTargetCol("target")\
    .setWeightCol("weight")
line.linkFrom(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.graph.LineBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LineBatchOpTest {
	@Test
	public void testLineBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1L", "5L", 1.),
			Row.of("2L", "5L", 1.),
			Row.of("3L", "5L", 1.),
			Row.of("4L", "5L", 1.),
			Row.of("1L", "6L", 1.),
			Row.of("2L", "6L", 1.),
			Row.of("3L", "6L", 1.),
			Row.of("4L", "6L", 1.),
			Row.of("7L", "6L", 15.),
			Row.of("7L", "8L", 1.),
			Row.of("7L", "9L", 1.));
		BatchOperator <?> data = new MemSourceBatchOp(df, "source string, target string, weight double");
		BatchOperator <?> line = new LineBatchOp()
			.setOrder("firstorder")
			.setRho(.025)
			.setVectorSize(5)
			.setNegative(5)
			.setIsToUndigraph(false)
			.setMaxIter(20)
			.setSampleRatioPerPartition(2.)
			.setSourceCol("source")
			.setTargetCol("target")
			.setWeightCol("weight");
		line.linkFrom(data).print();
	}
}
```

### 运行结果

|vertexId|vertexVector|
|--------|------------|
|10L|0.21389429778837246,0.1911353696863806,0.1316112606087454,-0.15504651922643958,0.9361386397244865|
|1L|0.43122351608756165,0.29783837576159716,-0.6242134421932172,-0.5699927640850769,0.10394425704541377|
|2L|0.46132608248237156,0.35541855098613856,-0.5135636636216717,-0.6265741465013136,0.06717962176150442|
|3L|0.22368818387133887,0.36608001332291756,-0.8030241335180479,-0.34202409002367046,0.23263873940705357|
|4L|0.3570337202077478,0.7126398702420819,-0.4696611127705199,-0.23265871937843668,-0.2999328215189313|
|5L|0.45792127623648854,0.47070594164899743,-0.56933846250774,-0.47612043020847256,0.13381730946965611|
|6L|-0.5015833272182806,-0.4481268255333818,0.4612455253782732,0.43140895120924494,-0.385662282611449|
|7L|-0.3152196336701024,-0.4607786197664082,0.6574313951991989,0.48164878283999957,0.15529989282105744|
|8L|-0.42030975318452385,0.05099491454249831,0.4269511935747453,-0.2071107702180848,0.7717234201665388|
|9L|-0.22769540933018892,-0.5154933470780315,0.5261821838436239,0.5103449434077099,0.3809222464388005|
