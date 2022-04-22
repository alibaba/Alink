# 二分K均值聚类 (BisectingKMeans)
Java 类名：com.alibaba.alink.pipeline.clustering.BisectingKMeans

Python 类名：BisectingKMeans


## 功能介绍
二分k均值算法是k-means聚类算法的一个变体，主要是为了改进k-means算法随机选择初始质心的随机性造成聚类结果不确定性的问题.

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |  |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN", "COSINE" | "EUCLIDEAN" |
| k | 聚类中心点数目 | 聚类中心点数目 | Integer |  |  | 4 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 10。 | Integer |  |  | 10 |
| minDivisibleClusterSize | 最小可分裂的聚类大小 | 最小可分裂的聚类大小 | Integer |  |  | 1 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  |  | 0 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |


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

kmeans = BisectingKMeans()\
    .setVectorCol("vec")\
    .setK(2)\
    .setPredictionCol("pred")

kmeans.fit(inOp)\
    .transform(inOp)\
    .print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.clustering.BisectingKMeans;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BisectingKMeansTest {
	@Test
	public void testBisectingKMeans() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(1, "0.1,0.1,0.1"),
			Row.of(2, "0.2,0.2,0.2"),
			Row.of(3, "9 9 9"),
			Row.of(4, "9.1 9.1 9.1"),
			Row.of(5, "9.2 9.2 9.2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, vec string");
		BisectingKMeans kmeans = new BisectingKMeans()
			.setVectorCol("vec")
			.setK(2)
			.setPredictionCol("pred");
		kmeans.fit(inOp)
			.transform(inOp)
			.print();
	}
}
```

### 运行结果
#### 预测结果
id|vec|pred
---|---|----
0|0 0 0|0
1|0.1,0.1,0.1|0
2|0.2,0.2,0.2|0
3|9 9 9|1
4|9.1 9.1 9.1|1
5|9.2 9.2 9.2|1
