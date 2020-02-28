## 功能介绍
给定切分点，将连续变量分桶，可支持单列输入或多列输入，对应需要给出单列切分点或者多列切分点。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| handleInvalid | 未知Token处理策略 | 未知Token处理策略，"keep", "skip", "error" | String | | "keep" |
| encode | 编码方式 | 编码方式，"INDEX", "VECTOR", "ASSEMBLED_VECTOR" | String |   |INDEX |
| dropLast | 是否删除最后一个元素 | 是否删除最后一个元素 | Boolean |  | true |
| leftOpen | 是否左开右闭 | 是否左开右闭 | Boolean | | true |
| cutsArray | 多列的切分点 | 多列的切分点 | double[][] | ✓ |  |

<!-- This is the end of auto-generated parameter info -->

## 脚本示例
#### 脚本代码
```
import numpy as np
import pandas as pd
data = np.array([
    [1.1, True, "2", "A"],
    [1.1, False, "2", "B"],
    [1.1, True, "1", "B"],
    [2.2, True, "1", "A"]
])
df = pd.DataFrame({"double": data[:, 0], "bool": data[:, 1], "number": data[:, 2], "str": data[:, 3]})

inOp = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
bucketizer = Bucketizer().setSelectedCols(["double"]).setCutsArray([[2]])
bucketizer.transform(inOp).print()
```
#### 脚本运行结果

##### 输出数据
```
rowID   double   bool  number str
0       0   True       2   A
1       0  False       2   B
2       0   True       1   B
3       1   True       1   A
```