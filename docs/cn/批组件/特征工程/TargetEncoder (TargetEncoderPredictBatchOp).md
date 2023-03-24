# TargetEncoder (TargetEncoderPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.TargetEncoderPredictBatchOp

Python 类名：TargetEncoderPredictBatchOp


## 功能介绍

特征编码预测。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，必选 | String[] | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |

