## 功能介绍
该功能由训练和预测组成，支持计算1. 求最近邻topN 2. 求radius范围内的邻居。该功能由预测时候的topN和radius参数控制, 如果填写了topN，则输出最近邻，如果填写了radius，则输出radius范围内的邻居。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| metric | 距离度量方式 | 距离类型 | String |  | "EUCLIDEAN" |
| solver | 近似方法 | 近似方法，包括KDTREE和LSH | String |  | "KDTREE" |
| radius | radius值 | radius值 | Double |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | null |
| idCol | id列名 | id列名 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| numHashTables | 哈希表的数目 | 哈希表的数目 | Integer |  | 1 |
| numProjectionsPerTable | 每个哈希表中的哈希函数个数 | 每个哈希表中的哈希函数个数 | Integer |  | 1 |
| seed | 采样种子 | 采样种子 | Long |  | 0 |
| projectionWidth | 桶的宽度 | 桶的宽度 | Double |  | 1.0 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


## 脚本示例
#### 脚本代码
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "0 0 0"],
    [1, "1 1 1"],
    [2, "2 2 2"]
])
df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})
inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
pipeline = VectorApproxNearestNeighbor().setIdCol("id").setSelectedCol("vec").setTopN(3)
pipeline.fit(inOp).transform(inOp).print()
```

#### 脚本输出结果
   id                                                vec
   
0   0  {"ID":"[0,1,2]","METRIC":"[0.0,1.7320508075688...

1   1  {"ID":"[1,2,0]","METRIC":"[0.0,1.7320508075688...

2   2  {"ID":"[2,1,0]","METRIC":"[0.0,1.7320508075688...



