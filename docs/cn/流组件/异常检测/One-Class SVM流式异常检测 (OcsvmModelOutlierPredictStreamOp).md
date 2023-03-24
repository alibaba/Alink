# One-Class SVM流式异常检测 (OcsvmModelOutlierPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.outlier.OcsvmModelOutlierPredictStreamOp

Python 类名：OcsvmModelOutlierPredictStreamOp


## 功能介绍
* 与传统SVM不同的是，one-class SVM是一种非监督的学习算法，经常被用来做异常点检测。在该算法的训练集中只有一类positive（或者negative）的数据，而没有（或存在极少量）另外一类，通常称其为异常点。该算法需要学习（learn）的就是边界（boundary），而不是最大间隔（maximum margin），通过边界对异常点进行预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

