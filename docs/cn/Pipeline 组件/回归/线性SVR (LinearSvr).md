# 线性SVR (LinearSvr)
Java 类名：com.alibaba.alink.pipeline.regression.LinearSvr

Python 类名：LinearSvr


## 功能介绍
* 线性SVR是一个回归算法
* 线性SVR组件支持稀疏、稠密两种数据格式
* 线性SVR组件支持带样本权重的训练

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| C | 算法参数 | 支撑向量回归参数 | Double | ✓ |  |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | x >= 0.0 | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  |  | null |
| learningRate | 学习率 | 优化算法的学习率，默认0.1。 | Double |  |  | 0.1 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | x >= 1 | 100 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| optimMethod | 优化方法 | 优化问题求解时选择的优化方法 | String |  | "LBFGS", "GD", "Newton", "SGD", "OWLQN" | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| standardization | 是否正则化 | 是否对训练数据做正则化，默认true | Boolean |  |  | true |
| tau | 算法参数 | 支撑向量回归参数 | Double |  |  | 0.1 |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  |  | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  |  | true |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])

batchData = BatchOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')

colnames = ["f0","f1"]

lsvr = LinearSvr()\
            .setFeatureCols(colnames)\
            .setLabelCol("label")\
            .setPredictionCol("pred")
    
model = lsvr.fit(batchData)

model.transform(batchData).print()
```
### 运行结果
|  f0 | f1 | label   |   pred|
|-----|----|---------|-------|
| 2 |  1   |   1 | 1.000014|
|  3 |  2  |    1 | 1.538474|
|   4 |  3  |    2 | 2.076934|
|  2 |  4  |    1 | 1.138446|
|  2 |  2  |    1 | 1.046158|
|  4 |  3  |    2 | 2.076934|
|  1 |  2  |    1 | 0.553842|
| 5 |  3  |    3 | 2.569250|

