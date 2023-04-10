# AutoCross预测 (AutoCrossPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.AutoCrossPredictBatchOp

Python 类名：AutoCrossPredictBatchOp


## 功能介绍
参考论文："AutoCross: Automatic Feature Crossing for Tabular Data in Real-World Applications"

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputFormat | 输出格式 | 输出格式 | String |  | "Dense", "Sparse", "Word" | "Sparse" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |

