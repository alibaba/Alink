# 网页流量指标生成 (WebTrafficIndexStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.WebTrafficIndexStreamOp

Python 类名：WebTrafficIndexStreamOp


## 功能介绍

- 网页流量通常用来构造特征

- 目前支持uv，pv，uip的计算


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| keyCol | 键值列 | 键值列 | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| bit | 比特数 | 比特数 | Integer |  |  | 10 |
| format | 计数方式 | 计数方式 | String |  | "SPARSE", "NORMAL" | "NORMAL" |
| index | 指标 | 指标 | String |  | "PV", "UV", "UIP", "CARDINALITY_ESTIMATE_STOCHASTIC", "CARDINALITY_ESTIMATE_LINEAR", "CARDINALITY_ESTIMATE_LOGLOG", "CARDINALITY_ESTIMATE_ADAPTIVE", "CARDINALITY_ESTIMATE_HYPERLOGLOG", "CARDINALITY_ESTIMATE_HYPERLOGLOGPLUS" | "PV" |
| timeInterval | 时间间隔 | 流式数据统计的时间间隔 | Double |  |  | 3.0 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

data = RandomTableSourceStreamOp()\
        .setNumCols(2)\
        .setMaxRows(50)\
        .setIdCol("id")\
        .setOutputCols(["f0", "f1"]) \
        .setOutputColConfs("f0:uniform_open(1,2);f1:uniform(1,2)")\
        .setTimePerSample(0.1)

op = WebTrafficIndexStreamOp()\
        .setTimeInterval(1)\
        .setSelectedCol("f0")\
        .linkFrom(data)

op.print()

StreamOperator.execute()
```

### 运行结果
```
1   window  8
2   all 506
3   window  10
4   all 516
5   window  10
```
