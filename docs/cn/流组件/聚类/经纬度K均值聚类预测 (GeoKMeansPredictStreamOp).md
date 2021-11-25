# 经纬度K均值聚类预测 (GeoKMeansPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.clustering.GeoKMeansPredictStreamOp

Python 类名：GeoKMeansPredictStreamOp


## 功能介绍

KMeans 是一个经典的聚类算法。

基本思想是：以空间中k个点为中心进行聚类，对最靠近他们的对象归类。通过迭代的方法，逐次更新各聚类中心的值，直至得到最好的聚类结果。

本组件主要针对经纬度距离做Kmeans聚类，包括经纬度KMeans，经纬度KMeans预测, 经纬度KMeans流式预测。

### 经纬度距离（[https://en.wikipedia.org/wiki/Haversine_formula](https://en.wikipedia.org/wiki/Haversine_formula))
<div align=center><img src="https://img.alicdn.com/tfs/TB1WD.qa5_1gK0jSZFqXXcpaXXa-63-4.svg"></div>

<div align=center><img src="https://img.alicdn.com/tfs/TB1RRApa.Y1gK0jSZFMXXaWcVXa-33-6.svg"></div>

输入数据中的经度和纬度使用`度数`表示，得到的距离单位为千米(km)。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| predictionDistanceCol | 预测距离列名 | 预测距离列名 | String |  |  |
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
    [0, 0],
    [8, 8],
    [1, 2],
    [9, 10],
    [3, 1],
    [10, 7]
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 long, f1 long')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='f0 long, f1 long')

kmeans = GeoKMeansTrainBatchOp()\
                .setLongitudeCol("f0")\
                .setLatitudeCol("f1")\
                .setK(2)\
                .linkFrom(inOp1)

kmeans.print()

predict = GeoKMeansPredictBatchOp()\
                .setPredictionCol("pred")\
                .linkFrom(kmeans, inOp1)
predict.print()

predict = GeoKMeansPredictStreamOp(kmeans)\
                .setPredictionCol("pred")\
                .linkFrom(inOp2)
predict.print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.KMeans4LongiLatitudePredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.KMeans4LongiLatitudeTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.clustering.KMeans4LongiLatitudePredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GeoKMeansPredictStreamOpTest {
	@Test
	public void testGeoKMeansPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, 0),
			Row.of(8, 8),
			Row.of(1, 2),
			Row.of(9, 10),
			Row.of(3, 1),
			Row.of(10, 7)
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "f0 int, f1 int");
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "f0 int, f1 int");
		BatchOperator <?> kmeans = new GeoKMeansTrainBatchOp()
			.setLongitudeCol("f0")
			.setLatitudeCol("f1")
			.setK(2)
			.linkFrom(inOp1);
		kmeans.print();
		BatchOperator <?> result = new GeoKMeansPredictBatchOp()
			.setPredictionCol("pred")
			.linkFrom(kmeans, inOp1);
		result.print();
		StreamOperator <?> predict = new GeoKMeansPredictStreamOp(kmeans)
			.setPredictionCol("pred")
			.linkFrom(inOp2);
		predict.print();
		StreamOperator.execute();
	}
}
```
### 运行结果
#### 模型数据
model_id|model_info
--------|----------
0|{"vectorCol":null,"latitudeCol":"\"f1\"","longitudeCol":"\"f0\"","distanceType":"\"HAVERSINE\"","k":"2","vectorSize":"2"}
1048576|{"clusterId":0,"weight":3.0,"center":"[8.333333333333332, 9.0]","vec":null}
2097152|{"clusterId":1,"weight":3.0,"center":"[1.0, 1.3333333333333333]","vec":null}


#### 预测输出
f0|f1|pred
---|---|----
0|0|1
8|8|0
1|2|1
9|10|0
3|1|1
10|7|0
