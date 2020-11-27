# Fm 回归

## 功能介绍

* Fm 回归算法，支持模型训练和样本预测。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  | true |
| withLinearItem | 是否含有线性项 | 是否含有线性项 | Boolean |  | true |
| numFactor | 因子数 | 因子数 | Integer |  | 10 |
| lambda0 | 常数项正则化系数 | 常数项正则化系数 | Double |  | 0.0 |
| lambda1 | 线性项正则化系数 | 线性项正则化系数 | Double |  | 0.0 |
| lambda2 | 二次项正则化系数 | 二次项正则化系数 | Double |  | 0.0 |
| numEpochs | epoch数 | epoch数 | Integer |  | 10 |
| learnRate | 学习率 | 学习率 | Double |  | 0.01 |
| initStdev | 初始化参数的标准差 | 初始化参数的标准差 | Double |  | 0.05 |

## 脚本示例
#### 运行脚本
```python
import numpy as np
import pandas as pd
data = np.array([
    ["1:1.1 3:2.0", 1.0],
    ["2:2.1 10:3.1", 1.0],
    ["1:1.2 5:3.2", 0.0],
    ["3:1.2 7:4.2", 0.0]])
df = pd.DataFrame({"kv": data[:, 0], 
                   "label": data[:, 1]})

input = dataframeToOperator(df, schemaStr='kv string, label double', op_type='batch')
test = dataframeToOperator(df, schemaStr='kv string, label double', op_type='stream')
# load data
fm = FmRegressor().setVectorCol("kv").setLabelCol("label").setPredictionCol("pred")
model = fm.fit(input)
model.transform(test).print()
StreamOperator.execute()
```
#### 运行结果
kv	| label	| pred
---|----|-------
1:1.1 3:2.0|1.0|0.473600
2:2.1 10:3.1|1.0|0.755115
1:1.2 5:3.2|0.0|0.005875
3:1.2 7:4.2|0.0|0.004641





