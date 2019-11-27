# 功能介绍
对输入表的每一行向量，在映射表中找到topN个最近邻的向量，输入表若为K行，输出则最多为K*topN行。
## 参数说明
<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| distanceType | 距离度量方式 | 聚类使用的距离类型，支持EUCLIDEAN 和 JACCARD | String |  | "EUCLIDEAN" |
| topN | 前N的数据 | 挑选最近的N个数据 | Integer |  | 5 |
| leftCol | 左表文本列名 | 左表文本列名 | String | ✓ |  |
| rightCol | 右表文本列名 | 右表文本列名 | String | ✓ |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| leftIdCol | 左侧ID列 | 左侧ID列 | String | ✓ |  |
| rightIdCol | 右侧ID列 | 右侧ID列 | String | ✓ |  |
| projectionWidth | 桶的宽度 | 桶的宽度 | Double |  | 1.0 |
| numHashTables | 哈希表的数目 | 哈希表的数目 | Integer |  | 1 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| numProjectionsPerTable | 每个哈希表中的哈希函数个数 | 每个哈希表中的哈希函数个数 | Integer |  | 1 |
| seed | 采样种子 | 采样种子 | Long |  | 0 |<!-- This is the end of auto-generated parameter info -->

## 脚本示例
#### 脚本代码
```
import numpy as np
import pandas as pd
data = np.array([
    [0, "0 0 0"],
    [1, "1 1 1"],
    [2, "2 2 2"]
])
df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})

source = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
op = (
    ApproxVectorSimilarityTopNLSHBatchOp()
    .setLeftIdCol("id")
    .setRightIdCol("id")
    .setLeftCol("vec")
    .setRightCol("vec")
    .setOutputCol("output"))
op.linkFrom(source, source).collectToDataframe()
```

#### 脚本运行结果

##### 输出数据
```
rowID id_right id_left output	rank
0	0	0	0.0	1
1	1	1	0.0	1
2	2	2	0.0	1
```