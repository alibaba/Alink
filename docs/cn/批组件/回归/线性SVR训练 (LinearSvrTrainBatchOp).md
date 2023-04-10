# 线性SVR训练 (LinearSvrTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.LinearSvrTrainBatchOp

Python 类名：LinearSvrTrainBatchOp


## 功能介绍
* 线性SVR是一个回归算法
* 线性SVR组件支持稀疏、稠密两种数据格式
* 线性SVR组件支持带样本权重的训练

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| C | 算法参数 | 支撑向量回归参数 | Double | ✓ |  |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | x >= 0.0 | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| learningRate | 学习率 | 优化算法的学习率，默认0.1。 | Double |  |  | 0.1 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | x >= 1 | 100 |
| optimMethod | 优化方法 | 优化问题求解时选择的优化方法 | String |  | "LBFGS", "GD", "Newton", "SGD", "OWLQN" | null |
| standardization | 是否正则化 | 是否对训练数据做正则化，默认true | Boolean |  |  | true |
| tau | 算法参数 | 支撑向量回归参数 | Double |  |  | 0.1 |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  |  | true |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [16.3, 1.1, 1.1],
    [16.8, 1.4, 1.5],
    [19.2, 1.7, 1.8],
    [18.0, 1.7, 1.7],
    [19.5, 1.8, 1.9],
    [20.9, 1.8, 1.8],
    [21.1, 1.9, 1.8],
    [20.9, 2.0, 2.1],
    [20.3, 2.3, 2.4],
    [22.0, 2.4, 2.5]
])

batchSource = BatchOperator.fromDataframe(df, schemaStr=' y double, x1 double, x2 double')

lsvr = LinearSvrTrainBatchOp()\
    .setFeatureCols(["x1", "x2"])\
    .setLabelCol("y")\
    .setC(1.0)\
    .setTau(0.01)

model = batchSource.link(lsvr)

predictor = LinearSvrPredictBatchOp()\
    .setPredictionCol("pred")

predictor.linkFrom(model, batchSource).print()
```
### 运行结果
 y | x1 | x2 | pred
---|----|----|-----
16.3|1.1|1.1|16.48073043727051
16.8|1.4|1.5|17.236649847389877
19.2|1.7|1.8|18.651637270539197
18.0|1.7|1.7|19.31070528356914
19.5|1.8|1.9|19.123299744922306
20.9|1.8|1.8|19.78236775795225
21.1|1.9|1.8|20.913098245365298
20.9|2.0|2.1|20.066624693688514
20.3|2.3|2.4|21.481612116837834
22.0|2.4|2.5|21.953274591220936



