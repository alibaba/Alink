# 多层感知机训练

## 功能介绍
多层感知机多分类模型

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| layers | 神经网络层大小 | 神经网络层大小 | int[] | ✓ |  |
| blockSize | 数据分块大小，默认值64 | 数据分块大小，默认值64 | Integer |  | 64 |
| initialWeights | 初始权重值 | 初始权重值 | DenseVector |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | 100 |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | 1.0E-6 |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | 0.0 |
| l2 | 正则化系数 | L2 正则化系数，默认为0。 | Double |  | 0.0 |



## 脚本示例
### 脚本代码
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = {
  'f1': np.random.rand(8),
  'f2': np.random.rand(8),
  'label': np.random.randint(low=1, high=3, size=8)
}

df_data = pd.DataFrame(data)
schema = 'f1 double, f2 double, label bigint'
train_data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')

mlpc = MultilayerPerceptronTrainBatchOp() \
  .setFeatureCols(["f1", "f2"]) \
  .setLabelCol("label") \
  .setLayers([2, 8, 3]) \
  .setMaxIter(10)

mlpc.linkFrom(train_data).print()
resetEnv()

```

### 脚本运行结果

```
           model_id                                         model_info  label_value
0                 0  {"vectorCol":null,"isVectorInput":"false","lay...          NaN
1           1048576  {"data":[2.330192598639656,1.895916109869876,1...          NaN
2  2251799812636672                                                NaN            1
3  2251799812636673                                                NaN            2
```
