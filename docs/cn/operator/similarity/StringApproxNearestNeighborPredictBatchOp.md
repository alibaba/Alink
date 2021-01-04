## 功能介绍

该功能由训练和预测组成，支持计算1. 求最近邻topN 2. 求radius范围内的邻居。该功能由预测时候的topN和radius参数控制, 如果填写了topN，则输出最近邻，如果填写了radius，则输出radius范围内的邻居。

SimhashHamming（SimHash_Hamming_Distance)相似度=1-距离/64.0，应选择metric的参数为SIMHASH_HAMMING_SIM。

MinHash应选择metric的参数为MINHASH_SIM。

Jaccard应选择metric的参数为JACCARD_SIM。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| radius | radius值 | radius值 | Double |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | null |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |



## 脚本示例
#### 脚本代码
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "abcde", "aabce"],
    [1, "aacedw", "aabbed"],
    [2, "cdefa", "bbcefa"],
    [3, "bdefh", "ddeac"],
    [4, "acedm", "aeefbc"]
])
df = pd.DataFrame({"id": data[:, 0], "text1": data[:, 1], "text2": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='batch')

train = StringApproxNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1").setMetric("SIMHASH_HAMMING_SIM").linkFrom(inOp)
predict = StringApproxNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3).linkFrom(train, inOp)
predict.print()
```
#### 脚本运行结果
   id   text1                                              text2
   
0   0   abcde  {"ID":"[0,1,2]","METRIC":"[0.953125,0.921875,0...

1   1  aacedw  {"ID":"[0,1,4]","METRIC":"[0.9375,0.90625,0.85...

2   2   cdefa  {"ID":"[0,1,4]","METRIC":"[0.890625,0.859375,0...

3   3   bdefh  {"ID":"[4,2,1]","METRIC":"[0.9375,0.90625,0.89...

4   4   acedm  {"ID":"[1,0,4]","METRIC":"[0.921875,0.921875,0...
