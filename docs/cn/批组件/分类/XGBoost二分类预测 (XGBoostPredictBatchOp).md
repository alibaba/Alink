# XGBoost二分类预测 (XGBoostPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.XGBoostPredictBatchOp

Python 类名：XGBoostPredictBatchOp


## 功能介绍
XGBoost 是一种使用广泛的 gradient boosting 方法。

### 文献或出处
1. [XGBoost Documentation](https://xgboost.readthedocs.io/en/stable/)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| pluginVersion | 插件版本号 | 插件版本号 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例

### Python 代码

### Java 代码

### 运行结果
