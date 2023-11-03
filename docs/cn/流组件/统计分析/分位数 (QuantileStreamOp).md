# 分位数 (QuantileStreamOp)
Java 类名：com.alibaba.alink.operator.stream.statistics.QuantileStreamOp

Python 类名：QuantileStreamOp


## 功能介绍

分位数将一列数据按大小排序，给出每个分位点（默认百分位）的值。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| quantileNum | 分位个数 | 分位个数 | Integer | ✓ | x >= 0 |  |
| dalayTime | 延迟时间 | 延迟时间 | Integer |  |  | 0 |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| timeCol | 时间列 | 时间列。如果用户输入时间列，则以此作为数据的时间；否则按照process time作为数据的时间。 | String |  |  | null |
| timeInterval | 时间间隔 | 流式数据统计的时间间隔 | Double |  |  | 3.0 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
        [0.0,0.0,0.0],
        [0.1,0.2,0.1],
        [0.2,0.2,0.8],
        [9.0,9.5,9.7],
        [9.1,9.1,9.6],
        [9.2,9.3,9.9]
        ])


streamData = StreamOperator.fromDataframe(df_data, schemaStr='x1 double, x2 double, x3 double')


quanOp = QuantileStreamOp()\
    .setSelectedCols(["x2","x3"])\
    .setQuantileNum(5)

#control data speed, 1 per second.      
speedControl = SpeedControlStreamOp()\
    .setTimeInterval(.3)

streamData.link(speedControl).link(quanOp).print()

StreamOperator.execute()
```

### 运行结果
starttime|endtime|colname|quantile
---------|-------|-------|--------
2020/03/23 22:42:45|2020/03/23 22:42:46|col2|[-99.9,-2.5,-2.5,1.3,1.3,100.2]
2020/03/23 22:42:45|2020/03/23 22:42:46|col3|[-0.01,0.9,0.9,1.1,1.1,100.9]









