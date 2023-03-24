# 异常检测评估 (EvalOutlierStreamOp)
Java 类名：com.alibaba.alink.operator.stream.evaluation.EvalOutlierStreamOp

Python 类名：EvalOutlierStreamOp


## 功能介绍
对异常检测结果进行评估。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String | ✓ |  |  |
| outlierValueStrings | 异常值 | 异常值，需要是字符串格式 | String[] |  |  |  |
| timeInterval | 时间间隔 | 流式数据统计的时间间隔 | Double |  |  | 3.0 |

