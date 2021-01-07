## 功能介绍

KMeans 是一个经典的聚类算法。

基本思想是：以空间中k个点为中心进行聚类，对最靠近他们的对象归类。通过迭代的方法，逐次更新各聚类中心的值，直至得到最好的聚类结果。

本组件主要针对经纬度距离做Kmeans聚类，包括[经纬度KMeans]，[经纬度KMeans预测], [经纬度KMeans流式预测]

### 经纬度距离（[https://en.wikipedia.org/wiki/Haversine_formula](https://en.wikipedia.org/wiki/Haversine_formula))
<div align=center><img src="https://img.alicdn.com/tfs/TB1WD.qa5_1gK0jSZFqXXcpaXXa-63-4.svg"></div>

<div align=center><img src="https://img.alicdn.com/tfs/TB1RRApa.Y1gK0jSZFMXXaWcVXa-33-6.svg"></div>

输入数据中的经度和纬度使用`度数`表示，得到的距离单位为千米(km)。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 50。 | Integer |  | 50 |
| initMode | 中心点初始化方法 | 初始化中心点的方法，支持"K_MEANS_PARALLEL"和"RANDOM" | String |  | "RANDOM" |
| initSteps | k-means++初始化迭代步数 | k-means初始化中心点时迭代的步数 | Integer |  | 2 |
| latitudeCol | 经度列名 | 经度列名 | String | ✓ |  |
| longitudeCol | 纬度列名 | 纬度列名 | String | ✓ |  |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  | 2 |
| epsilon | 收敛阈值 | 当两轮迭代的中心点距离小于epsilon时，算法收敛。 | Double |  | 1.0E-4 |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  | 0 |



## 脚本示例
#### 脚本代码
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, 0],
    [8, 8],
    [1, 2],
    [9, 10],
    [3, 1],
    [10, 7]
])
df = pd.DataFrame({"f0": data[:, 0], "f1": data[:, 1]})
inOp1 = dataframeToOperator(df, schemaStr='f0 long, f1 long', op_type='batch')
inOp2 = dataframeToOperator(df, schemaStr='f0 long, f1 long', op_type='stream')
kmeans = GeoKMeansTrainBatchOp().setLongitudeCol("f0").setLatitudeCol("f1").setK(2).linkFrom(inOp1)
kmeans.print()
predict = GeoKMeansPredictBatchOp().setPredictionCol("pred").linkFrom(kmeans, inOp1)
predict.print()

predict = GeoKMeansPredictStreamOp(kmeans).setPredictionCol("pred").linkFrom(inOp2)
predict.print()
StreamOperator.execute()
```
#### 脚本运行结果
##### 模型数据
model_id|model_info
--------|----------
0|{"vectorCol":null,"latitudeCol":"\"f1\"","longitudeCol":"\"f0\"","distanceType":"\"HAVERSINE\"","k":"2","vectorSize":"2"}
1048576|{"clusterId":0,"weight":3.0,"center":"[8.333333333333332, 9.0]","vec":null}
2097152|{"clusterId":1,"weight":3.0,"center":"[1.0, 1.3333333333333333]","vec":null}
2097152|{"center":"{\"data\":[1.0,1.3333333333333333]}","clusterId":1,"weight":3.0}


##### 预测输出
f0|f1|pred
---|---|----
0|0|1
8|8|0
1|2|1
9|10|0
3|1|1
10|7|0



