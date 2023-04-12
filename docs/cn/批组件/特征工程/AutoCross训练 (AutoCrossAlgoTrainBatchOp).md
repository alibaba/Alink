# AutoCross训练 (AutoCrossAlgoTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.AutoCrossAlgoTrainBatchOp

Python 类名：AutoCrossAlgoTrainBatchOp


## 功能介绍
参考论文："AutoCross: Automatic Feature Crossing for Tabular Data in Real-World Applications"

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |  |
| discreteThresholds | 离散个数阈值 | 离散个数阈值，低于该阈值的离散样本将不会单独成一个组别。 | Integer |  |  | -2147483648 |
| discreteThresholdsArray | 离散个数阈值数组 | 离散个数阈值，每一列对应数组中一个元素。 | Integer[] |  |  | null |
| fixCoefs | 固定的模型参数 | 固定的模型参数 | Boolean |  |  | false |
| fraction | 采样比例 | 采样比例 | Double |  | 0.0 <= x <= 1.0 | 0.8 |
| kCross | k折 | k折 | Integer |  | x >= 1 | 1 |
| maxSearchStep | 特征组合搜索步数 | 特征组合搜索步数 | Integer |  |  | 2 |

