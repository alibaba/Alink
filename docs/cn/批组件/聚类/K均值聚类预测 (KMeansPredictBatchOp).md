# K均值聚类预测 (KMeansPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.KMeansPredictBatchOp

Python 类名：KMeansPredictBatchOp


## 功能介绍

KMeans 是一个经典的聚类算法。

基本思想是：以空间中k个点为中心进行聚类，对最靠近他们的对象归类。通过迭代的方法，逐次更新各聚类中心的值，直至得到最好的聚类结果。

Alink上KMeans算法包括KMeans，KMeans批量预测, KMeans流式预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| predictionDistanceCol | 预测距离列名 | 预测距离列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



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

inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')

kmeans = KMeansTrainBatchOp()\
    .setVectorCol("vec")\
    .setK(2)\
    .linkFrom(inOp1)
kmeans.lazyPrint(10)

predictBatch = KMeansPredictBatchOp()\
    .setPredictionCol("pred")\
    .linkFrom(kmeans, inOp1)
predictBatch.print()

predictStream = KMeansPredictStreamOp(kmeans)\
    .setPredictionCol("pred")\
    .linkFrom(inOp2)
predictStream.print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeansPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.KMeansTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.KMeansPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KMeansPredictBatchOpTest {
	@Test
	public void testKMeansPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "0.1,0.1,0.1"),
			Row.of(2, "0.2,0.2,0.2"),
			Row.of(3, "9 9 9"),
			Row.of(4, "9.1 9.1 9.1"),
			Row.of(5, "9.2 9.2 9.2")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, vec string");
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "id int, vec string");
		BatchOperator <?> kmeans = new KMeansTrainBatchOp()
			.setVectorCol("vec")
			.setK(2)
            .linkFrom(inOp1);
		kmeans.lazyPrint(10);

		BatchOperator <?> predictBatch = new KMeansPredictBatchOp()
			.setPredictionCol("pred")
            .linkFrom(kmeans, inOp1);
		predictBatch.print();

		StreamOperator <?> predictStream = new KMeansPredictStreamOp(kmeans)
			.setPredictionCol("pred")
            .linkFrom(inOp2);
		predictStream.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
#### 模型结果
model_id|model_info
--------|----------
0|{"vectorCol":"\"vec\"","latitudeCol":null,"longitudeCol":null,"distanceType":"\"EUCLIDEAN\"","k":"2","vectorSize":"3"}
1048576|{"clusterId":0,"weight":3.0,"vec":{"data":[9.099999999999998,9.099999999999998,9.099999999999998]}}
2097152|{"clusterId":1,"weight":3.0,"vec":{"data":[0.1,0.1,0.1]}}

#### 预测结果
id|vec|pred
---|---|----
0|0 0 0|1
1|0.1,0.1,0.1|1
2|0.2,0.2,0.2|1
3|9 9 9|0
4|9.1 9.1 9.1|0
5|9.2 9.2 9.2|0
