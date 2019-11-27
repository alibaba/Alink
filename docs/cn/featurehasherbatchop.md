## 功能介绍
将多个特征组合成一个特征向量。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numFeatures | 向量维度 | 生成向量长度 | Integer |  | 262144 |
| categoricalCols | 离散特征列名 | 可选，默认选择String类型和Boolean类型作为离散特征，如果没有则为空 | String[] |  |  |<!-- This is the end of auto-generated parameter info -->

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

inOp1 = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')

hasher = FeatureHasherBatchOp().setSelectedCols(["double", "bool", "number", "str"]).setOutputCol("output").setNumFeatures(200)
hasher.linkFrom(inOp1).print()

hasher = FeatureHasherStreamOp().setSelectedCols(["double", "bool", "number", "str"]).setOutputCol("output").setNumFeatures(200)
hasher.linkFrom(inOp2).print()

StreamOperator.execute()
```

#### 脚本运行结果

##### 输出数据
```
   double   bool  number str                             output
0     1.1   True       2   A  $200$13:2.0 38:1.1 45:1.0 195:1.0
1     1.1  False       2   B   $200$13:2.0 30:1.0 38:1.1 76:1.0
2     1.1   True       1   B  $200$13:1.0 38:1.1 76:1.0 195:1.0
3     2.2   True       1   A  $200$13:1.0 38:2.2 45:1.0 195:1.0
```
