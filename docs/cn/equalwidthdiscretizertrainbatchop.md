# EqualWidthDiscretizer训练
## 功能介绍

等宽离散可以计算选定列的最大最小值，然后按照最大最小值进行同等距离离散化。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| leftOpen | 左开右闭 | 左开右闭 | Boolean | | true |
| numBuckets | quantile个数 | quantile个数，对所有列有效。 | Integer |  | 2 |
| numBucketsArray | quantile个数 | quantile个数，每一列对应数组中一个元素。 | Integer[] |  | null |<!-- This is the end of auto-generated parameter info -->

## 脚本示例

### 脚本代码

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
df = pd.DataFrame({"col0": data[:, 0], "col1": data[:, 1]})
inOp = dataframeToOperator(df, schemaStr='col0 long, col1 long', op_type='batch')
inOpStream = dataframeToOperator(df, schemaStr='col0 long, col1 long', op_type='stream')

train = EqualWidthDiscretizerTrainBatchOp().setNumBuckets(2).setSelectedCols(["col0"]).linkFrom(inOp)
EqualWidthDiscretizerPredictBatchOp().setSelectedCols(["col0"]).linkFrom(train, inOp).print()
EqualWidthDiscretizerPredictStreamOp(train).setSelectedCols(["col0"]).linkFrom(inOpStream).print()

StreamOperator.execute()
```

### 脚本结果
```
   col0  col1
0     0     0
1     1     8
2     0     2
3     1    10
4     0     1
5     1     7
```
