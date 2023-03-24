# 异常检测评估 (EvalOutlierBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalOutlierBatchOp

Python 类名：EvalOutlierBatchOp


## 功能介绍
对异常检测结果进行评估。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String | ✓ | 所选列类型为 [STRING] |  |
| outlierValueStrings | 异常值 | 异常值，需要是字符串格式 | String[] |  |  |  |

