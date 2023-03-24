# TargetEncoder (TargetEncoderTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.TargetEncoderTrainBatchOp

Python 类名：TargetEncoderTrainBatchOp


## 功能介绍

特征编码训练。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| positiveLabelValueString | 正样本 | 正样本对应的字符串格式。 | String |  |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |

