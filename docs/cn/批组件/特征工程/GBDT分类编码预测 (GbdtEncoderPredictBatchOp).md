# GBDT分类编码预测 (GbdtEncoderPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.GbdtEncoderPredictBatchOp

Python 类名：GbdtEncoderPredictBatchOp


## 功能介绍
使用GBDT模型，将输入数据编码为特征。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

