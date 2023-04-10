# DTW异常检测 (DynamicTimeWarpOutlierStreamOp)
Java 类名：com.alibaba.alink.operator.stream.outlier.DynamicTimeWarpOutlierStreamOp

Python 类名：DynamicTimeWarpOutlierStreamOp


## 功能介绍

DTW(Dynamic Time Warp)异常检测

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| direction | 方向 | 检测异常的方向 | String |  | "POSITIVE", "NEGATIVE", "BOTH" | "BOTH" |
| featureCol | 特征列名 | 特征列名，默认选最左边的列 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| historicalSeriesNum | 历史序列个数 | 历史序列个数 | Integer |  | x >= 2 | 1 |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| period | 周期 | 周期 | Integer |  | x >= 1 | 1 |
| precedingRows | 数据窗口大小 | 数据窗口大小 | Integer |  |  | null |
| precedingTime | 时间窗口大小 | 时间窗口大小 | String |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| searchWindow | 搜索窗口长度 | 搜错窗口长度 | Integer |  | x >= 1 | 1 |
| seriesLength | 序列长度 | 序列长度 | Integer |  | x >= 1 | 1 |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

