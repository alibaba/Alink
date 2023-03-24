# Som (SomBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.SomBatchOp

Python 类名：SomBatchOp


## 功能介绍

Self-Organized Map 算法，是一种高维数据可视化算法。

参考：[https://clarkdatalabs.github.io/soms/SOM_NBA](https://clarkdatalabs.github.io/soms/SOM_NBA)


## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| vdim | 向量长度 | 向量长度 | Integer | ✓ |  |  |
| vectorCol | vector列名 | vector列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |
| xdim | x方向网格数 | x方向网格数 | Integer | ✓ |  |  |
| ydim | y方向网格数 | y方向网格数 | Integer | ✓ |  |  |
| debug | 是否打开调试 | 是否打开调试 | Boolean |  |  | false |
| evaluation | 是否每轮评估迭代结果 | 是否每轮评估迭代结果 | Boolean |  |  | false |
| learnRate | 学习率 | 学习率 | Double |  |  | 0.5 |
| numIters | 迭代轮数 | 迭代轮数 | Integer |  |  | 100 |
| sigma | neighborhood函数方差 | neighborhood函数方差 | Double |  |  | 1.0 |




## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [5,2,3.5,1,'Iris-versicolor'],
    [5.1,3.7,1.5,0.4,'Iris-setosa'],
    [6.4,2.8,5.6,2.2,'Iris-virginica'],
    [6,2.9,4.5,1.5,'Iris-versicolor'],
    [4.9,3,1.4,0.2,'Iris-setosa'],
    [5.7,2.6,3.5,1,'Iris-versicolor'],
    [4.6,3.6,1,0.2,'Iris-setosa'],
    [5.9,3,4.2,1.5,'Iris-versicolor'],
    [6.3,2.8,5.1,1.5,'Iris-virginica'],
    [4.7,3.2,1.3,0.2,'Iris-setosa'],
    [5.1,3.3,1.7,0.5,'Iris-setosa'],
    [5.5,2.4,3.8,1.1,'Iris-versicolor'],
])

source = BatchOperator.fromDataframe(df, schemaStr='sepal_length double, sepal_width double, petal_length double, petal_width double, category string')

va = VectorAssemblerBatchOp().setSelectedCols(["sepal_length", "sepal_width"]) \
  .setOutputCol("features")

som = SomBatchOp()\
  .setXdim(2) \
  .setYdim(4) \
  .setVdim(2) \
  .setSigma(1.0) \
  .setNumIters(10) \
  .setVectorCol("features")

source.link(va).link(som).print()

```

### 运行结果

```
       meta  xidx  yidx              weights  cnt
0   4,4,2,r     0     0   6.098901,2.6216097    8
1   4,4,2,r     0     1  5.7773294,2.6960063   13
2   4,4,2,r     0     2  5.4155393,2.6422026    7
3   4,4,2,r     0     3   4.960137,2.6060686    6
4   4,4,2,r     1     0  6.3758574,2.8099446    9
5   4,4,2,r     1     1  6.1287007,2.9054923   11
6   4,4,2,r     1     2   5.422063,2.9557745    5
7   4,4,2,r     1     3  4.8237553,3.0189981   16
8   4,4,2,r     2     0   6.771961,2.9707642    8
9   4,4,2,r     2     1   6.541121,3.1317835   13
10  4,4,2,r     2     2     5.73227,3.323552    4
11  4,4,2,r     2     3  5.0270276,3.4258003   17
12  4,4,2,r     3     0   7.296618,3.1325939   11
13  4,4,2,r     3     1   6.956377,3.2311482    7
14  4,4,2,r     3     2  5.9284854,3.5446692    4
15  4,4,2,r     3     3   5.236388,3.7456865   11
```
