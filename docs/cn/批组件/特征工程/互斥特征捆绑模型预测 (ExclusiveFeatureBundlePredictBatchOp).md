# 互斥特征捆绑模型预测 (ExclusiveFeatureBundlePredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.ExclusiveFeatureBundlePredictBatchOp

Python 类名：ExclusiveFeatureBundlePredictBatchOp


## 功能介绍

对于稀疏的高维特征，许多特征是互斥的（即，这些特征从不同时取非零值），利用互斥的性质可以将互斥特征捆绑到一个特征中（称为Exclusive Feature Bundle），从而减少特征的数量。



## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| sparseVectorCol | 稀疏向量列名 | 稀疏向量列对应的列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |


