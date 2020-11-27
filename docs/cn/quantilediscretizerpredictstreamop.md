# QuantileDiscretizer训练
## 功能介绍

分位点离散可以计算选定列的分位点，然后使用这些分位点进行离散化。
生成选中列对应的q-quantile，其中可以所有列指定一个，也可以每一列对应一个

### 编码结果
##### Encode ——> INDEX
预测结果为单个token的index

##### Encode ——> VECTOR
预测结果为稀疏向量:

    1. dropLast为true,向量中非零元个数为0或者1
    2. dropLast为false,向量中非零元个数必定为1

##### Encode ——> ASSEMBLED_VECTOR
预测结果为稀疏向量,是预测选择列中,各列预测为VECTOR时,按照选择顺序ASSEMBLE的结果。

#### 向量维度
##### Encode ——> Vector
<div align=center><img src="http://latex.codecogs.com/gif.latex?vectorSize = numBuckets - dropLast(true: 1, false: 0) + (handleInvalid: keep(1), skip(0), error(0))" ></div>

    numBuckets: 训练参数

    dropLast: 预测参数

    handleInvalid: 预测参数

#### Token index
##### Encode ——> Vector

    1. 正常数据: 唯一的非零元为数据所在的bucket,若 dropLast为true, 最大的bucket的值会被丢掉，预测结果为全零元

    2. null: 
        2.1 handleInvalid为keep: 唯一的非零元为:numBuckets - dropLast(true: 1, false: 0)
        2.2 handleInvalid为skip: null
        2.3 handleInvalid为error: 报错

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP" |
| encode | 编码方法 | 编码方法 | String |  | "INDEX" |
| dropLast | 是否删除最后一个元素 | 删除最后一个元素是为了保证线性无关性。默认true | Boolean |  | true |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 脚本示例

### 脚本代码

```python
import numpy as np
import pandas as pd
from pyalink.alink import *


def exampleData():
    return np.array([
        ["a", 1, 1, 2.0, True],
        ["c", 1, 2, -3.0, True],
        ["a", 2, 2, 2.0, False],
        ["c", 0, 0, 0.0, False]
    ])


def sourceFrame():
    data = exampleData()
    return pd.DataFrame({
        "f_string": data[:, 0],
        "f_long": data[:, 1],
        "f_int": data[:, 2],
        "f_double": data[:, 3],
        "f_boolean": data[:, 4]
    })


def batchSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f_string string, 
    f_long long, 
    f_int int, 
    f_double double, 
    f_boolean boolean
    ''',
        op_type='batch'
    )


def streamSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f_string string, 
    f_long long, 
    f_int int, 
    f_double double, 
    f_boolean boolean
    ''',
        op_type='stream'
    )


trainOp = (
    QuantileDiscretizerTrainBatchOp()
    .setSelectedCols(['f_double'])
    .setNumBuckets(8)
    .linkFrom(batchSource())
)

predictBatchOp = (
    QuantileDiscretizerPredictBatchOp()
    .setSelectedCols(['f_double'])
)

(
    predictBatchOp
    .linkFrom(
        trainOp,
        batchSource()
    )
    .print()
)

predictStreamOp = (
    QuantileDiscretizerPredictStreamOp(
        trainOp
    )
    .setSelectedCols(['f_double'])
)

(
    predictStreamOp
    .linkFrom(
        streamSource()
    )
    .print()
)

StreamOperator.execute()
```

### 脚本结果
批预测结果
```
f_string  f_long  f_int  f_double  f_boolean
0        a       1      1         2       True
1        c       1      2         0       True
2        a       2      2         2      False
3        c       0      0         1      False
```
流预测结果
```
f_string    f_long  f_int   f_double    f_boolean
0   a   1   1   2   True
1   a   2   2   2   False
2   c   1   2   0   True
3   c   0   0   1   False
```
