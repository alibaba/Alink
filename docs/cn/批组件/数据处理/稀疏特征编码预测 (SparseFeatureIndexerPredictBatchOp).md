# 稀疏特征编码预测 (SparseFeatureIndexerPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.SparseFeatureIndexerPredictBatchOp

Python 类名：SparseFeatureIndexerPredictBatchOp


## 功能介绍

稀疏特征编码预测

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| handleDuplicate | 重复特征处理策略 | 重复特征处理策略。"first"最终value以第一个出现的特征为准, "last"最终value以最后一个出现的为准， "error"表示抛异常 | String |  | "FIRST", "LAST", "ERROR" | "FIRST" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

