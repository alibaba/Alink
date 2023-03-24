# cross候选特征选择预测 (CrossCandidateSelectorPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.CrossCandidateSelectorPredictBatchOp

Python 类名：CrossCandidateSelectorPredictBatchOp


## 功能介绍

从交叉特征候选集中，根据评估指标进行选择。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| appendOriginalData | Not available! | Not available! | Boolean |  |  | true |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputFormat | Not available! | Not available! | String |  | "Dense", "Sparse", "Word" | "Sparse" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |

