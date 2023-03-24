# Kmodes训练 (KModes)
Java 类名：com.alibaba.alink.pipeline.clustering.KModes

Python 类名：KModes


## 功能介绍

KModes是一种用于离散数据/分类数据(categorical data)的聚类算法。 基本思想是：把n个对象分为k个簇，使簇内具有较小的的相异度(或者称距离)。 距离计算方法：两个字符串比较，相同为0，不同为1。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  |  | 2 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numIter | 迭代次数 | 迭代次数，默认为10 | Integer |  |  | 10 |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
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
    ["pc", "Hp.com", 1],
    ["camera", "Hp.com", 1],
    ["digital camera", "Hp.com", 1],
    ["camera", "BestBuy.com", 1],
    ["digital camera", "BestBuy.com", 1],
    ["tv", "BestBuy.com", 1],
    ["flower", "Teleflora.com", 1],
    ["flower", "Orchids.com", 1]
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 string, f1 string')

kmodes = KModes()\
    .setFeatureCols(["f0", "f1"])\
    .setK(2)\
    .setPredictionCol("pred")
    
kmodes.fit(inOp1).transform(inOp1).print()
```

### 运行结果
```
               f0             f1  pred
0              pc         Hp.com     1
1          camera         Hp.com     1
2  digital camera         Hp.com     1
3          camera    BestBuy.com     0
4  digital camera    BestBuy.com     0
5              tv    BestBuy.com     0
6          flower  Teleflora.com     0
7          flower    Orchids.com     0
```

