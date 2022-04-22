# XGBoost二分类训练 (XGBoostTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.XGBoostTrainBatchOp

Python 类名：XGBoostTrainBatchOp


## 功能介绍
XGBoost 是一种使用广泛的 gradient boosting 方法。

### 文献或出处
1. [XGBoost Documentation](https://xgboost.readthedocs.io/en/stable/)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| numRound | 树的棵树 | 树的棵树 | Integer | ✓ |  |  |
| pluginVersion | 插件版本号 | 插件版本号 | String | ✓ |  |  |
| alpha | L1 正则项 | L1 正则项 | Double |  |  | 1.0 |
| baseScore | Base score | Base score | Double |  |  | 0.5 |
| colSampleByLevel | 每个树列采样 | 每个树列采样 | Double |  |  | 1.0 |
| colSampleByNode | 每个结点列采样 | 每个结点采样 | Double |  |  | 1.0 |
| colSampleByTree | 每个树列采样 | 每个树列采样 | Double |  |  | 1.0 |
| eta | 学习率 | 学习率 | Double |  |  | 0.3 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| gamma | 结点分裂最小损失变化 | 节点分裂最小损失变化 | Double |  |  | 0.0 |
| growPolicy | GrowPolicy | GrowPolicy | String |  | "DEPTH_WISE", "LOSS_GUIDE" | "DEPTH_WISE" |
| interactionConstraints | interaction constraints | interaction constraints | String |  |  | null |
| lambda | L2 正则项 | L2 正则项 | Double |  |  | 1.0 |
| maxBin | 最大结点个数 | 最大结点个数 | Integer |  |  | 256 |
| maxDeltaStep | Delta step | Delta step | Double |  |  | 0.0 |
| maxDepth | 最大深度 | 最大深度 | Integer |  |  | 6 |
| maxLeaves | 最大结点个数 | 最大结点个数 | Integer |  |  | 0 |
| minChildWeight | 结点的最小权重 | 结点的最小权重 | Double |  |  | 1.0 |
| monotoneConstraints | monotone constraints | monotone constraints | String |  |  | null |
| numClass | 标签类别个数 | 标签类别个数， 多分类时有效 | Integer |  |  | 0 |
| objective | objective | objective | String |  | "BINARY_LOGISTIC", "BINARY_LOGITRAW", "BINARY_HINGE", "MULTI_SOFTMAX", "MULTI_SOFTPROB" | "BINARY_LOGISTIC" |
| processType | ProcessType | ProcessType | String |  | "DEFAULT", "UPDATE" | "DEFAULT" |
| refreshLeaf | RefreshLeaf | RefreshLeaf | Integer |  |  | 1 |
| runningMode | 运行模式 | XGBoost的运行模型，ICQ速度快，但使用内存多，TRIAVIAL速度略慢，但是节省内存，按照流式方式处理。由于训练数据本身在XGBoost运行时已经被缓存进内存，所以存两份和存一份数据的资源消耗和速度对比，还需要进一步的测试。 | String |  | "ICQ", "TRIVIAL" | "TRIVIAL" |
| samplingMethod | 采样方法 | 采样方法 | String |  | "UNIFORM", "GRADIENT_BASED" | "UNIFORM" |
| scalePosWeight | ScalePosWeight | ScalePosWeight | Double |  |  | 1.0 |
| singlePrecisionHistogram | single precision histogram | single precision histogram | Boolean |  |  | false |
| sketchEps | SketchEps | SketchEps | Double |  |  | 0.03 |
| subSample | 样本采样比例 | 样本采样比例 | Double |  |  | 1.0 |
| treeMethod | 构建树的方法 | 构建树的方法 | String |  | "AUTO", "EXACT", "APPROX", "HIST" | "AUTO" |
| updater | Updater | Updater | String |  |  | "grow_colmaker,prune" |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |

## 代码示例

### Python 代码

### Java 代码

### 运行结果
