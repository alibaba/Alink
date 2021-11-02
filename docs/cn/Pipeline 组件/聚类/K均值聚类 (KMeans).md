# K均值聚类 (KMeans)
Java 类名：com.alibaba.alink.pipeline.clustering.KMeans

Python 类名：KMeans


## 功能介绍
KMeans 是一个经典的聚类算法。

基本思想是：以空间中k个点为中心进行聚类，对最靠近他们的对象归类。通过迭代的方法，逐次更新各聚类中心的值，直至得到最好的聚类结果。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 50。 | Integer |  | 50 |
| initMode | 中心点初始化方法 | 初始化中心点的方法，支持"K_MEANS_PARALLEL"和"RANDOM" | String |  | "RANDOM" |
| initSteps | k-means++初始化迭代步数 | k-means初始化中心点时迭代的步数 | Integer |  | 2 |
| epsilon | 收敛阈值 | 当两轮迭代的中心点距离小于epsilon时，算法收敛。 | Double |  | 1.0E-4 |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  | 2 |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN" |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| predictionDistanceCol | 预测距离列名 | 预测距离列名 | String |  |  |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  | 0 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |



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

inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')

kmeans = KMeans()\
    .setVectorCol("vec")\
    .setK(2)\
    .setPredictionCol("pred")

kmeans.fit(inOp).transform(inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.clustering.KMeans;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KMeansTest {
	@Test
	public void testKMeans() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "0.1,0.1,0.1"),
			Row.of(2, "0.2,0.2,0.2"),
			Row.of(3, "9 9 9"),
			Row.of(4, "9.1 9.1 9.1"),
			Row.of(5, "9.2 9.2 9.2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, vec string");
		KMeans kmeans = new KMeans()
			.setVectorCol("vec")
			.setK(2)
			.setPredictionCol("pred");
		kmeans.fit(inOp).transform(inOp).print();
	}
}
```

### 运行结果
#### 预测结果
id|vec|pred
---|---|----
0|0 0 0|1
1|0.1,0.1,0.1|1
2|0.2,0.2,0.2|1
3|9 9 9|0
4|9.1 9.1 9.1|0
5|9.2 9.2 9.2|0
