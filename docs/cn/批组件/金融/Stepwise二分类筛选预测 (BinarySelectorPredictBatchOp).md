# Stepwise二分类筛选预测 (BinarySelectorPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.finance.BinarySelectorPredictBatchOp

Python 类名：BinarySelectorPredictBatchOp


## 功能介绍

使用Stepwise逻辑回归方法，进行特征筛选。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| selectedCol | 计算列对应的列名 | 计算列对应的列名, 默认值是null | String |  |  | null |




## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["$3$0:1.0 1:7.0 2:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 1.0],
    ["$3$0:1.0 1:3.0 2:3.0", "2.0 3.0 3.0", 2.0, 3.0, 3.0, 1.0],
    ["$3$0:1.0 1:2.0 2:4.0", "3.0 2.0 4.0", 3.0, 2.0, 4.0, 0.0],
    ["$3$0:1.0 1:3.0 2:4.0", "2.0 3.0 4.0", 2.0, 3.0, 4.0, 0.0],
    ["$3$0:1.0 1:3.0 2:4.0", "1.0 5.0 8.0", 1.0, 5.0, 8.0, 0.0],
    ["$3$0:1.0 1:3.0 2:4.0", "1.0 6.0 3.0", 1.0, 6.0, 3.0, 0.0]
])

batchData = BatchOperator.fromDataframe(df, schemaStr='svec string, vec string, f0 double, f1 double, f2 double, label double')

selector = BinarySelectorTrainBatchOp()\
                .setAlphaEntry(0.65)\
                .setAlphaStay(0.7)\
                .setSelectedCol("vec")\
                .setLabelCol("label")\
                .setForceSelectedCols([1])

batchData.link(selector)

predict = BinarySelectorPredictBatchOp()\
            .setPredictionCol("pred")\
            .setReservedCols(["label"])

predict.linkFrom(selector, batchData).print()
```

### 运行结果

```
   label     pred
0    1.0  7.0 9.0
1    1.0  3.0 3.0
2    0.0  2.0 4.0
3    0.0  3.0 4.0
4    0.0  5.0 8.0
5    0.0  6.0 3.0
```





