## 功能介绍
* 线性回归是一个回归算法
* 线性回归组件支持稀疏、稠密两种数据格式
* 线性回归组件支持带样本权重的训练

## 参数说明





| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| optimMethod | 优化方法 | 优化问题求解时选择的优化方法 | String |  | null |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | 0.0 |
| l2 | 正则化系数 | L2 正则化系数，默认为0。 | Double |  | 0.0 |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  | true |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | 100 |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| standardization | 是否正则化 | 是否对训练数据做正则化，默认true | Boolean |  | true |



## 参数填写示例
#### 训练参数
![](https://img.alicdn.com/tfs/TB1fEuDbeL2gK0jSZPhXXahvXXa-257-601.png)
#### 预测参数
![](https://img.alicdn.com/tfs/TB1qRmFbeP2gK0jSZFoXXauIVXa-270-231.png)


## 脚本示例

### 脚本代码
```python
import numpy as np
import pandas as pd
from pyalink.alink import *

data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})

batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
colnames = ["f0","f1"]
lr = LinearRegTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(lr)

predictor = LinearRegPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()
```

### 运行结果
f0 | f1 | label | pred
---|----|-------|-----
   2 |  1   |   1  | 1.000014
   3 |  2   |   1  | 1.538474
   4 |  3   |   2  | 2.076934
   2 |  4   |   1  | 1.138446
   2 |  2   |   1  | 1.046158
   4 |  3   |   2  | 2.076934
   1 |  2   |   1  | 0.553842
   5 |  3   |   3  | 2.569250


