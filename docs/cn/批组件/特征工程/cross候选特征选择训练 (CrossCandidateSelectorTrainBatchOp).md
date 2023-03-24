# cross候选特征选择训练 (CrossCandidateSelectorTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.CrossCandidateSelectorTrainBatchOp

Python 类名：CrossCandidateSelectorTrainBatchOp


## 功能介绍

从交叉特征候选集中，根据评估指标进行选择。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| crossFeatureNumber | Not available! | Not available! | Integer | ✓ |  |  |
| featureCandidates | Not available! | Not available! | String[] | ✓ |  |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| binningMethod | 连续特征分箱方法 | 连续特征分箱方法 | String |  | "QUANTILE", "BUCKET" | "QUANTILE" |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |  |
| discreteThresholds | 离散个数阈值 | 离散个数阈值，低于该阈值的离散样本将不会单独成一个组别。 | Integer |  |  | -2147483648 |
| discreteThresholdsArray | 离散个数阈值数组 | 离散个数阈值，每一列对应数组中一个元素。 | Integer[] |  |  | null |
| fixCoefs | Not available! | Not available! | Boolean |  |  | false |
| fraction | Not available! | Not available! | Double |  | [0.0, 1.0] | 0.8 |
| kCross | Not available! | Not available! | Integer |  | [1, +inf) | 1 |
| maxSearchStep | 特征组合搜索步数 | 特征组合搜索步数 | Integer |  |  | 2 |
| numBuckets | quantile个数 | quantile个数，对所有列有效。 | Integer |  |  | 2 |
| numBucketsArray | quantile个数 | quantile个数，每一列对应数组中一个元素。 | Integer[] |  |  | null |

