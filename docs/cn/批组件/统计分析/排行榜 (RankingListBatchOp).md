# 排行榜 (RankingListBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.RankingListBatchOp

Python 类名：RankingListBatchOp


## 功能介绍

排行榜是用来计算分组榜单的，例如数据是

<div align=center>
    <img src="https://img.alicdn.com/tfs/TB19Sq1b.Y1gK0jSZFMXXaWcVXa-664-592.png" height="50%" width="50%">
</div>


选择marital（婚姻状况）作为分组列， age（年龄）作为主体列， balance(净资产)作为计算列，计算指标是sum，&lt;br /&gt;
那么结果

| marital | age | sum(balance) | rank |
| :--- | :--- | :--- | :--- |
| divorced | 54 | 318166.0 | 1 |
| divorced | 56 | 283257.0 | 2 |
| divorced | 59 | 281327.0 | 3 |
| divorced | 58 | 263003.0 | 4 |
| married | 37 | 1389347.0 | 1 |
| married | 45 | 1372091.0 | 2 |
| married | 39 | 1301587.0 | 3 |
| married | 36 | 1274481.0 | 4 |
| single | 32 | 1365481.0 | 1 |
| single | 31 | 1348609.0 | 2 |
| single | 30 | 1287184.0 | 3 |
| single | 33 | 1044254.0 | 4 |


可以看出，离婚人群中50岁往上的资产比较多，结婚人群中35到45岁资产比较多，单身的30到35资产比较多。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| objectCol | 主体列 | 主体列 | String | ✓ |  |  |
| addedCols | 附加列 | 附加列 | String[] |  |  | null |
| addedStatTypes | 附加列统计类型 | 附加列统计类型 | String[] |  |  | null |
| groupCol | 分组单列名 | 分组单列名，可选 | String |  |  | null |
| groupValues | 计算分组 | 计算分组, 分组列选择时必选, 用逗号分隔 | String[] |  |  | null |
| isDescending | 是否降序 | 是否降序 | Boolean |  |  | false |
| statCol | 计算列 | 计算列 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| statType | 统计类型 | 统计类型 | String |  | "count", "countTotal", "min", "max", "sum", "mean", "variance" | "count" |
| topN | 个数 | 个数 | Integer |  |  | 10 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
        ["1", "a", 1.3, 1.1],
        ["1", "b", -2.5, 0.9],
        ["2", "c", 100.2, -0.01],
        ["2", "d", -99.9, 100.9],
        ["1", "a", 1.4, 1.1],
        ["1", "b", -2.2, 0.9],
        ["2", "c", 100.9, -0.01],
        ["2", "d", -99.5, 100.9]
])

batchData = BatchOperator.fromDataframe(df, schemaStr='id string, col1 string, col2 double, col3 double')

rankList = RankingListBatchOp()\
			.setGroupCol("id")\
			.setGroupValues(["1", "2"])\
			.setObjectCol("col1")\
			.setStatCol("col2")\
			.setStatType("sum")\
			.setTopN(20)
        

batchData.link(rankList).print()
```

### 运行结果

|id |col1   |col2  |rank|
|:--|:------|:-----|:---|
|1  |  b | -4.7  |   1|
|1  |  a  |  2.7  |   2|
|2  |  d |-199.4 |    1|
|2  |  c  |201.1 |    2|

