## 功能介绍
给定切分点，将连续变量分桶，可支持单列输入或多列输入，对应需要给出单列切分点或者多列切分点。

每列切分点需要严格递增，且至少有三个点。


<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| handleInvalid | 如何处理无效值 | 可以选择skip：跳过，error：报错抛异常。 | String |  | "error" |
| selectedCols | 计算列对应的列名列表 | 计算列对应的列名列表 | String[] |  |  |
| splitsArray | 多列的切分点 | 多列的切分点 | String[] |  |  |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->

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

bucketizer = BucketizerBatchOp().setSelectedCols(["double"]).setSplitsArray(["-Infinity:2:Infinity"])
bucketizer.linkFrom(inOp1).print()

bucketizer = BucketizerStreamOp().setSelectedCols(["double"]).setSplitsArray(["-Infinity:2:Infinity"])
bucketizer.linkFrom(inOp2).print()

StreamOperator.execute()
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