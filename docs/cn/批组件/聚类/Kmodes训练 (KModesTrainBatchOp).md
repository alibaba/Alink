# Kmodes训练 (KModesTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.KModesTrainBatchOp

Python 类名：KModesTrainBatchOp


## 功能介绍

KModes是一种用于离散数据/分类数据(categorical data)的聚类算法。 基本思想是：把n个对象分为k个簇，使簇内具有较小的的相异度(或者称距离)。 距离计算方法：两个字符串比较，相同为0，不同为1。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  |  | 2 |
| numIter | 迭代次数 | 迭代次数，默认为10 | Integer |  |  | 10 |


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
inOp2 = StreamOperator.fromDataframe(df, schemaStr='f0 string, f1 string')

kmodes = KModesTrainBatchOp()\
    .setFeatureCols(["f0", "f1"])\
    .setK(2)\
    .linkFrom(inOp1)
    
predict = KModesPredictBatchOp()\
    .setPredictionCol("pred")\
    .linkFrom(kmodes, inOp1)
    
kmodes.lazyPrint(10)
predict.print()

predict = KModesPredictStreamOp(kmodes)\
    .setPredictionCol("pred")\
    .linkFrom(inOp2)
    
predict.print()

StreamOperator.execute()
```

### 运行结果
##### 模型结果
```
model_id                                         model_info
0         0                  {"featureCols":"[\"f0\",\"f1\"]"}
1   1048576  {"center":"[\"camera\",\"BestBuy.com\"]","clus...
2   2097152  {"center":"[\"flower\",\"Hp.com\"]","clusterId...
```

##### 预测结果
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

