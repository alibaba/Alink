# GBDT分类编码训练 (GbdtEncoderTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.GbdtEncoderTrainBatchOp

Python 类名：GbdtEncoderTrainBatchOp


## 功能介绍
使用GBDT模型，将输入数据编码为特征。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  | 所选列类型为 [BOOLEAN, DATE, DOUBLE, FLOAT, INTEGER, LONG, SHORT, STRING, TIME, TIMESTAMP] |  |
| criteria | 树分裂的策略 | 树分裂的策略，可以为PAI, XGBOOST | String |  | "PAI", "XGBOOST" | "PAI" |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BOOLEAN, DATE, DOUBLE, FLOAT, INTEGER, LONG, SHORT, STRING, TIME, TIMESTAMP] | null |
| featureImportanceType | 特征重要性类型 | 特征重要性类型（默认为GAIN） | String |  | "WEIGHT", "GAIN", "COVER" | "GAIN" |
| featureSubsamplingRatio | 每棵树特征采样的比例 | 每棵树特征采样的比例，范围为(0, 1]。 | Double |  |  | 1.0 |
| gamma | xgboost中的l2正则项 | xgboost中的l2正则项 | Double |  |  | 0.0 |
| lambda | xgboost中的l1正则项 | xgboost中的l1正则项 | Double |  |  | 0.0 |
| learningRate | 学习率 | 学习率（默认为0.3） | Double |  |  | 0.3 |
| maxBins | 连续特征进行分箱的最大个数 | 连续特征进行分箱的最大个数。 | Integer |  |  | 128 |
| maxDepth | 树的深度限制 | 树的深度限制 | Integer |  |  | 6 |
| maxLeaves | 叶节点的最多个数 | 叶节点的最多个数 | Integer |  |  | 2147483647 |
| minInfoGain | 分裂的最小增益 | 分裂的最小增益 | Double |  |  | 0.0 |
| minSampleRatioPerChild | 子节点占父节点的最小样本比例 | 子节点占父节点的最小样本比例 | Double |  |  | 0.0 |
| minSamplesPerLeaf | 叶节点的最小样本个数 | 叶节点的最小样本个数 | Integer |  |  | 100 |
| minSumHessianPerLeaf | 叶子节点最小Hessian值 | 叶子节点最小Hessian值（默认为0） | Double |  |  | 0.0 |
| newtonStep | 是否使用二阶梯度 | 是否使用二阶梯度 | Boolean |  |  | true |
| numTrees | 模型中树的棵数 | 模型中树的棵数 | Integer |  |  | 100 |
| subsamplingRatio | 每棵树的样本采样比例或采样行数 | 每棵树的样本采样比例或采样行数，行数上限100w行 | Double |  |  | 1.0 |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |

