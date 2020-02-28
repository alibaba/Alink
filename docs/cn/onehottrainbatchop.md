# one-hot编码组件

## 算法介绍

one-hot编码，也称独热编码，对于每一个特征，如果它有m个可能值，那么经过 独热编码后，就变成了m个二元特征。并且，这些特征互斥，每次只有一个激活。 因此，数据会变成稀疏的，输出结果也是kv的稀疏结构。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| discreteThresholdsArray | 离散个数阈值 | 离散个数阈值，每一列对应数组中一个元素 | Integer[] |  | |
| discreteThresholds | 离散个数阈值 | 离散个数阈值，低于该阈值的离散样本将不会单独成一个组别 | Integer |  | Integer.MIN_VALUE |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |

<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 运行脚本
```python
import numpy as np
import pandas as pd
data = np.array([
    [1.1, True, "2", "A"],
    [1.1, False, "2", "B"],
    [1.1, True, "1", "B"],
    [2.2, True, "1", "A"]
])
df = pd.DataFrame({"double": data[:, 0], "bool": data[:, 1], "number": data[:, 2], "str": data[:, 3]})

inOp1 = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')

onehot = OneHotTrainBatchOp().setSelectedCols(["double", "bool", "number", "str"]).setDiscreteThresholds(2)
predictBatch = OneHotPredictBatchOp().setSelectedCols(["double", "bool"]).setEncode("ASSEMBLED_VECTOR").setOutputCols(["pred"]).setDropLast(False)
onehot.linkFrom(inOp1)
predictBatch.linkFrom(onehot, inOp1)
[model,predict] = collectToDataframes(onehot, predictBatch)
print(model)
print(predict)

predictStream = OneHotPredictStreamOp(onehot).setSelectedCols(["double", "bool"]).setEncode("ASSEMBLED_VECTOR").setOutputCols(["vec"])
predictStream.linkFrom(inOp2)
predictStream.print(refreshInterval=-1)
StreamOperator.execute()
```
#### 运行结果

```python
   double   bool  number str            pred
0     1.1   True       2   A  $6$0:1.0 3:1.0
1     1.1  False       2   B  $6$0:1.0 5:1.0
2     1.1   True       1   B  $6$0:1.0 3:1.0
3     2.2   True       1   A  $6$2:1.0 3:1.0

```





