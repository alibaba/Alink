# 二分K均值聚类训练 (BisectingKMeansTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.BisectingKMeansTrainBatchOp

Python 类名：BisectingKMeansTrainBatchOp


## 功能介绍
二分k均值算法是k-means聚类算法的一个变体，主要是为了改进k-means算法随机选择初始质心的随机性造成聚类结果不确定性的问题.

Alink上算法包括二分K均值聚类训练，二分K均值聚类预测, 二分K均值聚类流式预测。

## 参数说明
#### 训练
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| minDivisibleClusterSize | 最小可分裂的聚类大小 | 最小可分裂的聚类大小 | Integer |  | 1 |
| k | 聚类中心点数目 | 聚类中心点数目 | Integer |  | 4 |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN" |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 10。 | Integer |  | 10 |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  | 0 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, "0 0 0"],
    [1, "0.1,0.1,0.1"],
    [2, "0.2,0.2,0.2"],
    [3, "9 9 9"],
    [4, "9.1 9.1 9.1"],
    [5, "9.2 9.2 9.2"]
])

inBatch = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')

inStream = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')

kmeansTrain = BisectingKMeansTrainBatchOp()\
    .setVectorCol("vec")\
    .setK(2)
    
predictBatch = BisectingKMeansPredictBatchOp()\
    .setPredictionCol("pred")
    
kmeansTrain.linkFrom(inBatch)

predictBatch.linkFrom(kmeansTrain, inBatch)

kmeansTrain.lazyPrint(10)
predictBatch.print()

predictStream = BisectingKMeansPredictStreamOp(kmeansTrain)\
    .setPredictionCol("pred")
    
predictStream.linkFrom(inStream)

predictStream.print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.BisectingKMeansPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.BisectingKMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.BisectingKMeansPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BisectingKMeansTrainBatchOpTest {
	@Test
	public void testBisectingKMeansTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "0.1,0.1,0.1"),
			Row.of(2, "0.2,0.2,0.2"),
			Row.of(3, "9 9 9"),
			Row.of(4, "9.1 9.1 9.1"),
			Row.of(5, "9.2 9.2 9.2")
		);
		BatchOperator <?> inBatch = new MemSourceBatchOp(df, "id int, vec string");
		StreamOperator <?> inStream = new MemSourceStreamOp(df, "id int, vec string");
		BatchOperator <?> kmeansTrain = new BisectingKMeansTrainBatchOp()
			.setVectorCol("vec")
			.setK(2);
		BatchOperator <?> predictBatch = new BisectingKMeansPredictBatchOp()
			.setPredictionCol("pred");
		kmeansTrain.linkFrom(inBatch);
		predictBatch.linkFrom(kmeansTrain, inBatch);
		kmeansTrain.lazyPrint(10);
		predictBatch.print();
		StreamOperator <?> predictStream = new BisectingKMeansPredictStreamOp(kmeansTrain)
			.setPredictionCol("pred");
		predictStream.linkFrom(inStream);
		predictStream.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
#### 模型结果
model_id|model_info
--------|----------
0|{"vectorCol":"\"vec\"","distanceType":"\"EUCLIDEAN\"","k":"2","vectorSize":"3"}
1048576|{"clusterId":1,"size":6,"center":{"data":[4.6,4.6,4.6]},"cost":364.61999999999995}
2097152|{"clusterId":2,"size":3,"center":{"data":[0.1,0.1,0.1]},"cost":0.06}
3145728|{"clusterId":3,"size":3,"center":{"data":[9.099999999999998,9.099999999999998,9.099999999999998]},"cost":0.060000000000172804}

#### 预测结果
id|vec|pred
---|---|----
0|0 0 0|0
1|0.1,0.1,0.1|0
2|0.2,0.2,0.2|0
3|9 9 9|1
4|9.1 9.1 9.1|1
5|9.2 9.2 9.2|1




