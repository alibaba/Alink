## 功能介绍

相关系数算法用于计算一个矩阵中每一列之间的相关系数，范围在[-1,1]之间。计算的时候，count数按两列间同时非空的元素个数计算，两两列之间可能不同。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | null |
| method | 方法 | 方法：包含"PEARSON"和"SPEARMAN"两种，PEARSON。 | String |  | "PEARSON" |




## 脚本示例

### 脚本代码

```python
import numpy as np
import pandas as pd

data = np.array([
         [0.0,0.0,0.0],
         [0.1,0.2,0.1],
         [0.2,0.2,0.8],
         [9.0,9.5,9.7],
         [9.1,9.1,9.6],
         [9.2,9.3,9.9]])

df = pd.DataFrame({"x1": data[:, 0], "x2": data[:, 1], "x3": data[:, 2]})
source = dataframeToOperator(df, schemaStr='x1 double, x2 double, x3 double', op_type='batch')


corr = CorrelationBatchOp()\
            .setSelectedCols(["x1","x2","x3"])

corr = source.link(corr).collectCorrelation()
print(corr)

```
### 脚本运行结果

```
colName|x1|x2|x3
-------|--|--|--
x1|1.0000|0.9994|0.9990
x2|0.9994|1.0000|0.9986
x3|0.9990|0.9986|1.0000
```


