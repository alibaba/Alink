# FpGrowth

## 功能介绍
FP Growth(Frequent Pattern growth)算法是一种非时序的关联分析算法. 它利用FP tree生成频繁项集和规则,效率优于传统的Apriori算法。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemsCol | 项集列名 | 项集列名 | String | ✓ |  |
| minSupportCount | 最小支持度数目 | 最小支持度目，当取值大于或等于0时起作用，当小于0时参数minSupportPercent起作用 | Integer |  | -1 |
| minSupportPercent | 最小支持度占比 | 最小支持度占比，当minSupportCount取值小于0时起作用，当minSupportCount大于或等于0时该参数不起作用 | Double |  | 0.02 |
| minConfidence | 最小置信度 | 最小置信度 | Double |  | 0.05 |
| maxPatternLength | 最大频繁项集长度 | 最大频繁项集长度 | Integer |  | 10 |
| maxConsequentLength | 最大关联规则后继长度 | 最大关联规则后继(consequent)长度 | Integer |  | 1 |
| minLift | 最小提升度 | 最小提升度 | Double |  | 1.0 |



## 脚本示例
### 脚本代码
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = np.array([
    ["A,B,C,D"],
    ["B,C,E"],
    ["A,B,C,E"],
    ["B,D,E"],
    ["A,B,C,D"],
])

df_data = pd.DataFrame({
    "items": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='items string', op_type='batch')

fpGrowth = FpGrowthBatchOp() \
    .setItemsCol("items") \
    .setMinSupportPercent(0.4) \
    .setMinConfidence(0.6)

fpGrowth.linkFrom(data)

fpGrowth.print()
fpGrowth.getSideOutput(0).print()
resetEnv()

```

### 脚本运行结果

频繁项集输出：

```
    itemset  supportcount  itemcount
0         E             3          1
1       B,E             3          2
2       C,E             2          2
3     B,C,E             2          3
4         D             3          1
5       B,D             3          2
6       C,D             2          2
7     B,C,D             2          3
8       A,D             2          2
9     B,A,D             2          3
10    C,A,D             2          3
11  B,C,A,D             2          4
12        A             3          1
13      B,A             3          2
14      C,A             3          2
15    B,C,A             3          3
16        C             4          1
17      B,C             4          2
18        B             5          1
```

关联规则输出：

```
        rule  itemcount      lift  support_percent  confidence_percent  transaction_count
0       B=>A          2  1.000000              0.6            0.600000                  3
1       B=>D          2  1.000000              0.6            0.600000                  3
2       B=>C          2  1.000000              0.8            0.800000                  4
3       B=>E          2  1.000000              0.6            0.600000                  3
4       C=>A          2  1.250000              0.6            0.750000                  3
5       C=>B          2  1.000000              0.8            1.000000                  4
6     B,C=>A          3  1.250000              0.6            0.750000                  3
7       A=>C          2  1.250000              0.6            1.000000                  3
8       A=>B          2  1.000000              0.6            1.000000                  3
9       A=>D          2  1.111111              0.4            0.666667                  2
10    B,A=>D          3  1.111111              0.4            0.666667                  2
11    C,A=>B          3  1.000000              0.6            1.000000                  3
12    B,A=>C          3  1.250000              0.6            1.000000                  3
13    C,A=>D          3  1.111111              0.4            0.666667                  2
14  B,C,A=>D          4  1.111111              0.4            0.666667                  2
15      D=>A          2  1.111111              0.4            0.666667                  2
16      D=>B          2  1.000000              0.6            1.000000                  3
17    A,D=>B          3  1.000000              0.4            1.000000                  2
18    B,D=>A          3  1.111111              0.4            0.666667                  2
19    C,D=>B          3  1.000000              0.4            1.000000                  2
20    A,D=>C          3  1.250000              0.4            1.000000                  2
21    C,D=>A          3  1.666667              0.4            1.000000                  2
22  C,A,D=>B          4  1.000000              0.4            1.000000                  2
23  B,A,D=>C          4  1.250000              0.4            1.000000                  2
24  B,C,D=>A          4  1.666667              0.4            1.000000                  2
25      E=>B          2  1.000000              0.6            1.000000                  3
26    C,E=>B          3  1.000000              0.4            1.000000                  2
```



