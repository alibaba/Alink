# SHEsd异常检测 (SHEsdOutlierBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.SHEsdOutlierBatchOp

Python 类名：SHEsdOutlierBatchOp


## 功能介绍

SHEsd异常检测

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String | ✓ |  |  |
| direction | 方向 | 检测异常的方向 | String |  | "POSITIVE", "NEGATIVE", "BOTH" | "BOTH" |
| frequency | 时序频率 | 时序频率 | Integer |  | [1, +inf) | 10 |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| maxIter | 最大迭代步数 | 最大迭代步数 | Integer |  |  |  |
| maxOutlierNumPerGroup | 每组最大异常点数目 | 每组最大异常点数目 | Integer |  |  |  |
| maxOutlierRatio | 最大异常点比例 | 算法检测异常点的最大比例 | Double |  |  |  |
| maxSampleNumPerGroup | 每组最大样本数目 | 每组最大样本数目 | Integer |  |  |  |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| shesdAlpha | Not available! | Not available! | Double |  | [0.0, +inf) | 0.05 |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


